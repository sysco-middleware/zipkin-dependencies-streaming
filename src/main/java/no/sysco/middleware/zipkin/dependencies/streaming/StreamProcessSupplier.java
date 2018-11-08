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

	StreamProcessSupplier() {
	}

	Topology build() {
		var builder = new StreamsBuilder();
		builder.stream("zipkin", Consumed.with(Serdes.String(), Serdes.String()))
				.mapValues(value -> SpanBytesDecoder.JSON_V2
						.decodeList(value.getBytes(Charset.defaultCharset())))
				.flatMapValues((readOnlyKey, value) -> value)
				.groupBy((key, value) -> value.traceId(),
						Serialized.with(Serdes.String(), new SpanSerde()))
				.aggregate(ArrayList::new,
						(String key, Span value, List<Span> aggregate) -> {
							aggregate.add(value);
							return aggregate;
						}, Materialized.with(Serdes.String(), new SpansSerde()))
				.toStream()
				.mapValues(
						value -> new DependencyLinker().putTrace(value.iterator()).link())
				.flatMapValues(value -> value)
				.groupBy(
						(key, value) -> String.format("%s-%s", value.parent(),
								value.child()),
						Serialized.with(Serdes.String(), new DependencyLinkSerde()))
				.reduce((l, r) -> DependencyLink.newBuilder().parent(l.parent())
						.child(l.child()).callCount(l.callCount() + r.callCount())
						.errorCount(l.errorCount() + r.errorCount()).build())
				.toStream().foreach((key, value) -> System.out.println(value));
		return builder.build();
	}

}
