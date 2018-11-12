package no.sysco.middleware.zipkin.dependencies.streaming;

import com.typesafe.config.ConfigFactory;
import no.sysco.middleware.zipkin.dependencies.streaming.storage.CassandraDepedencyStorage;
import no.sysco.middleware.zipkin.dependencies.streaming.storage.ElasticsearchDependencyStorage;
import no.sysco.middleware.zipkin.dependencies.streaming.storage.StdoutDependencyStorage;
import org.apache.kafka.streams.KafkaStreams;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ZipkinDependenciesStream {

	public static void main(String[] args) {
		final var config = ConfigFactory.load();
		final var appConfig = AppConfig.build(config);

		final var dependencyStorage = buildStorage(appConfig);
		final var streamProcess = new StreamProcessSupplier(appConfig.zipkin.format,
				dependencyStorage, appConfig.zipkin.topics.span,
				appConfig.zipkin.topics.dependency);
		final var topology = streamProcess.build();

		final var kafkaStreams = new KafkaStreams(topology,
				appConfig.kafkaStream.config());

		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

	static DependencyStorage buildStorage(AppConfig appConfig) {
		switch (appConfig.zipkin.storage.type) {
		case ELASTICSEARCH:
			final var restHighLevelClient = new RestHighLevelClient(
					RestClient.builder(appConfig.zipkin.storage.elasticsearch.nodes()));
			final var dateSeparator = appConfig.zipkin.storage.elasticsearch.dateSeparator;
			final var index = appConfig.zipkin.storage.elasticsearch.index;
			return new ElasticsearchDependencyStorage(restHighLevelClient, index,
					dateSeparator);
		case CASSANDRA:
			final var keyspace = appConfig.zipkin.storage.cassandra.keyspace;
			final var addresses = appConfig.zipkin.storage.cassandra.addresses;
			return new CassandraDepedencyStorage(keyspace, addresses);
		case STDOUT:
			return new StdoutDependencyStorage();
		}
		throw new IllegalStateException("Illegal storage type");
	}

}
