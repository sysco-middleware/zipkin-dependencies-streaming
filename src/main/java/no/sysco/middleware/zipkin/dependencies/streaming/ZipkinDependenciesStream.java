package no.sysco.middleware.zipkin.dependencies.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.streams.KafkaStreams;

public class ZipkinDependenciesStream {

	public static void main(String[] args) {
		final Config config = ConfigFactory.load();
		final var appConfig = AppConfig.build(config);

		final var streamProcess = new StreamProcessSupplier();
		final var topology = streamProcess.build();

		var kafkaStreams = new KafkaStreams(topology, appConfig.kafkaStream.config());

		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
