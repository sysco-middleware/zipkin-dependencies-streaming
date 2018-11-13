package no.sysco.middleware.zipkin.dependencies.streaming;

import com.typesafe.config.Config;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

class AppConfig {

	final KafkaStreams kafkaStreams;

	final String format;

	final Duration timeWindow;

	final Storage storage;

	private AppConfig(KafkaStreams kafkaStreams, String format, Duration timeWindow,
			Storage storage) {
		this.kafkaStreams = kafkaStreams;
		this.format = format;
		this.timeWindow = timeWindow;
		this.storage = storage;
	}

	static class KafkaStreams {

		final String bootstrapServers;

		final String applicationId;

		final Topics topics;

		KafkaStreams(String bootstrapServers, String applicationId, Topics topics) {
			this.bootstrapServers = bootstrapServers;
			this.applicationId = applicationId;
			this.topics = topics;
		}

		Properties config() {
			final var streamsConfig = new Properties();
			streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
			streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					EARLIEST.name().toLowerCase());
			return streamsConfig;
		}

		static class Topics {

			final String span;

			final String dependency;

			Topics(String span, String dependency) {
				this.span = span;
				this.dependency = dependency;
			}

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

			final String[] contactPoints;

			CassandraStorage(String keyspace, String contactPoints) {
				this.keyspace = keyspace;
				this.contactPoints = contactPoints.split(",");
			}

		}

	}

	enum StorageType {

		ELASTICSEARCH, CASSANDRA, STDOUT

	}

	static AppConfig build(Config config) {
		final var topics = new KafkaStreams.Topics(
				config.getString("kafka-streams.topics.span"),
				config.getString("kafka-streams.topics.dependency"));
		final var kafkaStream = new KafkaStreams(
				config.getString("kafka-streams.bootstrap-servers"),
				config.getString("kafka-streams.application-id"), topics);
		final var storageType = config.getEnum(StorageType.class, "storage.type");
		Storage.ElasticsearchStorage elasticseach = null;
		Storage.CassandraStorage cassandra = null;
		switch (storageType) {
		case ELASTICSEARCH:
			elasticseach = new Storage.ElasticsearchStorage(
					config.getString("storage.elasticsearch.index"),
					config.getString("storage.elasticsearch.urls"),
					config.getString("storage.elasticsearch.date-separator"));
			break;
		case CASSANDRA:
			cassandra = new Storage.CassandraStorage(
					config.getString("storage.cassandra.keyspace"),
					config.getString("storage.cassandra.contact-points"));
			break;
		}
		final var format = config.getString("format");
		final var timeWindow = config.getDuration("time-window");
		final var storage = new Storage(storageType, elasticseach, cassandra);
		return new AppConfig(kafkaStream, format, timeWindow, storage);
	}

}
