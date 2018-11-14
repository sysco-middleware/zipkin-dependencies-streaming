package no.sysco.middleware.zipkin.dependencies.streaming;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import zipkin2.DependencyLink;

import java.time.Duration;
import java.util.Properties;

import static org.junit.Assert.*;

public class StreamProcessSupplierTest {

	static class TestDependencyStorage implements DependencyStorage {

		@Override
		public void put(Long start, DependencyLink dependencyLink) {

		}

		@Override
		public void close() {

		}

	}

	public void test() {
		final var spanTopic = "";
		final var dependencyTopic = "";
		final var streamSupplier = new StreamProcessSupplier(new TestDependencyStorage(),
				spanTopic, dependencyTopic, Duration.ofSeconds(3));
		final var topology = streamSupplier.build();

		final var config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		final var testDriver = new TopologyTestDriver(topology, config);

		// TODO implement stream logic for testing
	}

}