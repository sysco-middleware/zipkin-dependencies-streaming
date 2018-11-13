package no.sysco.middleware.zipkin.dependencies.streaming;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.DependencyLinkSerde;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.SpanSerde;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.SpansSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;

class StreamProcessSupplier {

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

	StreamProcessSupplier(DependencyStorage dependencyStorage, String spanTopic,
			String dependencyTopic) {
		this(SpanBytesDecoder.JSON_V2.name(), dependencyStorage, spanTopic,
				dependencyTopic);
	}

	Topology build() {
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
				.toStream()
				.mapValues(
						value -> new DependencyLinker().putTrace(value.iterator()).link())
				.flatMapValues(value -> value)
				.groupBy(
						(key, value) -> String.format(DEPENDENCY_PAIR_PATTERN,
								value.parent(), value.child()),
						Serialized.with(Serdes.String(), dependencyLinkSerde))
				.windowedBy(TimeWindows.of(Duration.ofMinutes(5).toMillis()))
				.reduce((l, r) -> DependencyLink.newBuilder().parent(l.parent())
						.child(l.child()).callCount(l.callCount() + r.callCount())
						.errorCount(l.errorCount() + r.errorCount()).build())
				.toStream()
				.map((key, value) -> KeyValue.pair(key.window().start(), value))
				.through(dependencyTopic,
						Produced.with(Serdes.Long(), dependencyLinkSerde))
				.foreach(dependencyStorage::put);
		return builder.build();
	}

}
