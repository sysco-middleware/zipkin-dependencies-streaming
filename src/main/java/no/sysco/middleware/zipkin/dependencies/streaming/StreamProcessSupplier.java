package no.sysco.middleware.zipkin.dependencies.streaming;

import no.sysco.middleware.zipkin.dependencies.streaming.serdes.DependencyLinkSerde;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.SpanSerde;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.SpansSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;

import java.nio.charset.Charset;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class StreamProcessSupplier {

	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

	private final SpanBytesDecoder spanBytesDecoder;

	private final SpanSerde spanSerde;

	private final SpansSerde spansSerde;

	private final DependencyStorage dependencyStorage;

	private final String spanTopic;

	private final String dependencyTopic;

	private final DependencyLinkSerde dependencyLinkSerde;

	StreamProcessSupplier(String format, DependencyStorage dependencyStorage,
			String spanTopic, String dependencyTopic) {
		this.dependencyStorage = dependencyStorage;
		this.spanTopic = spanTopic;
		this.dependencyTopic = dependencyTopic;
		this.spanBytesDecoder = SpanBytesDecoder.valueOf(format);
		this.spanSerde = new SpanSerde(format);
		this.spansSerde = new SpansSerde(format);
		this.dependencyLinkSerde = new DependencyLinkSerde();
	}

	public StreamProcessSupplier(DependencyStorage dependencyStorage, String spanTopic,
			String dependencyTopic) {
		this(SpanBytesDecoder.JSON_V2.name(), dependencyStorage, spanTopic,
				dependencyTopic);
	}

	public Topology build() {
		var builder = new StreamsBuilder();
		builder.stream(spanTopic, Consumed.with(Serdes.String(), Serdes.String()))
				.mapValues(value -> spanBytesDecoder.decodeList(value.getBytes(UTF_8)))
				.flatMapValues((readOnlyKey, value) -> value)
				.groupBy((key, value) -> value.traceId(),
						Serialized.with(Serdes.String(), spanSerde))
				.aggregate(ArrayList::new,
						(String key, Span value, List<Span> aggregate) -> {
							aggregate.add(value);
							return aggregate;
						}, Materialized.with(Serdes.String(), spansSerde))
				.toStream().filterNot((key, value) -> value.isEmpty())
				.mapValues(
						value -> new DependencyLinker().putTrace(value.iterator()).link())
				.flatMapValues(value -> value)
				.groupBy(
						(key, value) -> String.format(DEPENDENCY_PAIR_PATTERN,
								value.parent(), value.child()),
						Serialized.with(Serdes.String(), dependencyLinkSerde))
				.reduce((l, r) -> r,
						Materialized.with(Serdes.String(), dependencyLinkSerde))
				.toStream()
				.through(dependencyTopic,
						Produced.with(Serdes.String(), dependencyLinkSerde))
				.foreach((key, value) -> dependencyStorage
						.put(LocalDate.now().toEpochDay(), value));
		return builder.build();
	}

}
