package no.sysco.middleware.zipkin.dependencies.streaming;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

class AppConfig {

	final KafkaStream kafkaStream;

	private AppConfig(KafkaStream kafkaStream) {
		this.kafkaStream = kafkaStream;
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
			streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST.name().toLowerCase());
			return streamsConfig;
		}

	}

	static AppConfig build(Config config) {
		final KafkaStream kafkaStream = new KafkaStream(
				config.getString("kafka-streams.bootstrap-servers"),
				config.getString("kafka-streams.application-id"));
		return new AppConfig(kafkaStream);
	}

}
