package no.sysco.middleware.zipkin.dependencies.streaming;

import com.typesafe.config.ConfigFactory;
import no.sysco.middleware.zipkin.dependencies.streaming.storage.CassandraDependencyStorage;
import no.sysco.middleware.zipkin.dependencies.streaming.storage.ElasticsearchDependencyStorage;
import no.sysco.middleware.zipkin.dependencies.streaming.storage.StdoutDependencyStorage;
import org.apache.kafka.streams.KafkaStreams;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * Streaming version of Zipkin Dependencies application. It collect Span from a Kafka topic
 * and define Dependencies to be stored a Zipkin Storage.
 *
 * See more {@link AppConfig}
 */
public class ZipkinDependenciesStream {

	public static void main(String[] args) {
		final var config = ConfigFactory.load();
		final var appConfig = AppConfig.build(config);

		final var dependencyStorage = buildStorage(appConfig);
		final var streamProcess = new StreamProcessSupplier(appConfig.format,
				dependencyStorage, appConfig.kafkaStreams.topics.span,
				appConfig.kafkaStreams.topics.dependency);
		final var topology = streamProcess.build();

		final var kafkaStreams = new KafkaStreams(topology,
				appConfig.kafkaStreams.config());

		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
		Runtime.getRuntime().addShutdownHook(new Thread(dependencyStorage::close));
	}

	private static DependencyStorage buildStorage(AppConfig appConfig) {
		switch (appConfig.storage.type) {
		case ELASTICSEARCH:
			final var restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(appConfig.storage.elasticsearch.nodes()));
			final var dateSeparator = appConfig.storage.elasticsearch.dateSeparator;
			final var index = appConfig.storage.elasticsearch.index;
			return new ElasticsearchDependencyStorage(restHighLevelClient, index,
					dateSeparator);
		case CASSANDRA:
			final var keyspace = appConfig.storage.cassandra.keyspace;
			final var addresses = appConfig.storage.cassandra.contactPoints;
			return new CassandraDependencyStorage(keyspace, addresses);
		case STDOUT:
			return new StdoutDependencyStorage();
		}
		throw new IllegalStateException("Illegal storage type");
	}

}
