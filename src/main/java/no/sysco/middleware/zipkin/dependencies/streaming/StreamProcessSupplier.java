package no.sysco.middleware.zipkin.dependencies.streaming;

import no.sysco.middleware.zipkin.dependencies.streaming.serdes.DependencyLinkSerde;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.SpanSerde;
import no.sysco.middleware.zipkin.dependencies.streaming.serdes.SpansSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

class StreamProcessSupplier {

	static final Charset UTF_8 = Charset.forName("UTF-8");

	static final String DEPENDENCY_PAIR_PATTERN = "%s-%s";

	final SpanBytesDecoder spanBytesDecoder;

	final SpanSerde spanSerde;

	final SpansSerde spansSerde;

	StreamProcessSupplier(String format) {
		this.spanBytesDecoder = SpanBytesDecoder.valueOf(format);
		this.spanSerde = new SpanSerde(format);
		this.spansSerde = new SpansSerde(format);
	}

	StreamProcessSupplier() {
		this(SpanBytesDecoder.JSON_V2.name());
	}

	Topology build() {
		var builder = new StreamsBuilder();
		builder.stream("zipkin", Consumed.with(Serdes.String(), Serdes.String()))
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
						Serialized.with(Serdes.String(), new DependencyLinkSerde()))
				.reduce((l, r) -> DependencyLink.newBuilder().parent(l.parent())
						.child(l.child()).callCount(l.callCount() + r.callCount())
						.errorCount(l.errorCount() + r.errorCount()).build())
				.toStream().foreach((key, value) -> System.out.println(value));
		return builder.build();
	}

}
