package no.sysco.middleware.zipkin.dependencies.streaming;

import com.typesafe.config.Config;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

class AppConfig {

	final KafkaStream kafkaStream;

	final Zipkin zipkin;

	private AppConfig(KafkaStream kafkaStream, Zipkin zipkin) {
		this.kafkaStream = kafkaStream;
		this.zipkin = zipkin;
	}

	static class KafkaStream {

		final String bootstrapServers;

		final String applicationId;

		KafkaStream(String bootstrapServers, String applicationId) {
			this.bootstrapServers = bootstrapServers;
			this.applicationId = applicationId;
		}

		Properties config() {
			final var streamsConfig = new Properties();
			streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
			streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					EARLIEST.name().toLowerCase());
			return streamsConfig;
		}

	}

	static class Zipkin {

		final String format;

		final Storage storage;

		final Topics topics;

		Zipkin(String format, Storage storage, Topics topics) {
			this.format = format;
			this.topics = topics;
			this.storage = storage;
		}

		static class Topics {

			final String span;

			final String dependency;

			Topics(String span, String dependency) {
				this.span = span;
				this.dependency = dependency;
			}

		}

		static class Storage {

			final StorageType type;

			final ElasticsearchStorage elasticsearch;

			final CassandraStorage cassandra;

			Storage(StorageType type, ElasticsearchStorage elasticsearch,
					CassandraStorage cassandra) {
				this.type = type;
				this.elasticsearch = elasticsearch;
				this.cassandra = cassandra;
			}

			static class ElasticsearchStorage {

				final String index;

				final String urls;

				final String dateSeparator;

				ElasticsearchStorage(String index, String urls, String dateSeparator) {
					this.index = index;
					this.urls = urls;
					this.dateSeparator = dateSeparator;
				}

				HttpHost[] nodes() {
					return List.of(urls.split(",")).stream().map(HttpHost::create)
							.toArray(HttpHost[]::new);
				}

			}

			static class CassandraStorage {

				final String keyspace;

				final String[] addresses;

				CassandraStorage(String keyspace, String addresses) {
					this.keyspace = keyspace;
					this.addresses = addresses.split(",");
				}

			}

		}

		enum StorageType {

			ELASTICSEARCH, CASSANDRA, STDOUT

		}

	}

	static AppConfig build(Config config) {
		final KafkaStream kafkaStream = new KafkaStream(
				config.getString("kafka-streams.bootstrap-servers"),
				config.getString("kafka-streams.application-id"));
		final var storageType = config.getEnum(Zipkin.StorageType.class,
				"zipkin.storage.type");
		Zipkin.Storage.ElasticsearchStorage elasticseach = null;
		Zipkin.Storage.CassandraStorage cassandra = null;
		switch (storageType) {
		case ELASTICSEARCH:
			elasticseach = new Zipkin.Storage.ElasticsearchStorage(
					config.getString("zipkin.storage.elasticsearch.index"),
					config.getString("zipkin.storage.elasticsearch.urls"),
					config.getString("zipkin.storage.elasticsearch.date-separator"));
			break;
		case CASSANDRA:
			cassandra = new Zipkin.Storage.CassandraStorage(
					config.getString("zipkin.storage.cassandra.keyspace"),
					config.getString("zipkin.storage.cassandra.addresses"));
			break;
		}
		final Zipkin zipkin = new Zipkin(config.getString("zipkin.format"),
				new Zipkin.Storage(storageType, elasticseach, cassandra),
				new Zipkin.Topics(config.getString("zipkin.topics.span"),
						config.getString("zipkin.topics.dependency")));
		return new AppConfig(kafkaStream, zipkin);
	}

}
