package no.sysco.middleware.zipkin.dependencies.streaming;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Test;
import zipkin2.DependencyLink;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;

public class StreamProcessSupplierTest {

	static class TestDependencyStorage implements DependencyStorage {

		static List<DependencyLink> dependencyLinkList = new ArrayList<>();

		@Override
		public void put(Long start, DependencyLink dependencyLink) {
			dependencyLinkList.add(dependencyLink);
		}

		@Override
		public void close() {

		}

	}

	@Test
	public void should_createDependenciesFromInputFile() throws Exception {
		final var spanTopic = "spans";
		final var streamSupplier = new StreamProcessSupplier(new TestDependencyStorage(),
				spanTopic);
		final var topology = streamSupplier.build();

		final var config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		config.put(StreamsConfig.STATE_DIR_CONFIG,
				"target/kafka-streams-" + Instant.now().toEpochMilli());

		final var testDriver = new TopologyTestDriver(topology, config);

		final var factory = new ConsumerRecordFactory<>(spanTopic, new StringSerializer(),
				new StringSerializer());

		final var resource = this.getClass().getClassLoader().getResource("spans.json");
		final var uri = Objects.requireNonNull(resource).toURI();
		final var jsonBytes = Files.readAllBytes(Paths.get(uri));
		final var json = new String(jsonBytes, UTF_8);

		testDriver.pipeInput(factory.create(json));

		assertTrue(TestDependencyStorage.dependencyLinkList.stream()
				.anyMatch(dependencyLink -> dependencyLink.parent().equals("kafka")
						&& dependencyLink.child().equals("servicea")));
		assertTrue(TestDependencyStorage.dependencyLinkList.stream()
				.anyMatch(dependencyLink -> dependencyLink.parent().equals("servicea")
						&& dependencyLink.child().equals("kafka")));
		assertTrue(TestDependencyStorage.dependencyLinkList.stream()
				.anyMatch(dependencyLink -> dependencyLink.parent().equals("kafka")
						&& dependencyLink.child().equals("serviceb")));
	}

}