package no.sysco.middleware.zipkin.dependencies.streaming;

import no.sysco.middleware.zipkin.dependencies.streaming.serdes.DependencyLinkSerde;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Test;
import zipkin2.DependencyLink;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class StreamProcessSupplierTest {

	static class TestDependencyStorage implements DependencyStorage {

		@Override
		public void put(Long start, DependencyLink dependencyLink) {

		}

		@Override
		public void close() {

		}

	}

	@Test
	public void should_createDependenciesFromInputFile() throws Exception {
		final var spanTopic = "spans";
		final var dependencyTopic = "dependencies";
		final var streamSupplier = new StreamProcessSupplier(new TestDependencyStorage(),
				spanTopic, dependencyTopic);
		final var topology = streamSupplier.build();

		final var config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		config.put(StreamsConfig.STATE_DIR_CONFIG,
				"target/kafka-streams-" + Instant.now().toEpochMilli());

		final var dependencySerde = new DependencyLinkSerde();

		final var testDriver = new TopologyTestDriver(topology, config);

		final var factory = new ConsumerRecordFactory<>(spanTopic, new StringSerializer(),
				new StringSerializer());

		final var resource = this.getClass().getClassLoader().getResource("spans.json");
		final var uri = Objects.requireNonNull(resource).toURI();
		final var jsonBytes = Files.readAllBytes(Paths.get(uri));
		final var json = new String(jsonBytes, UTF_8);

		testDriver.pipeInput(factory.create(json));

		final var counters = new HashMap<String, Long>();

		var i = 0;
		ProducerRecord<String, DependencyLink> output;

		do {
			output = testDriver.readOutput(dependencyTopic,
					Serdes.String().deserializer(), dependencySerde.deserializer());
			if (output != null) {
				DependencyLink dependencyLink = output.value();
				System.out.println(i + " " + dependencyLink);
				i++;
				counters.put(dependencyLink.parent() + "|" + dependencyLink.child(), dependencyLink.callCount() );
			}
		}
		while (output != null);

		assertEquals(Long.valueOf(4L), counters.getOrDefault("kafka|servicea", 0L));
		assertEquals(Long.valueOf(9L), counters.getOrDefault("servicea|kafka", 0L));
		assertEquals(Long.valueOf(3L), counters.getOrDefault("kafka|serviceb", 0L));
	}

}