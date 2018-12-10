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

	private final DependencyLinkSerde dependencyLinkSerde;

	StreamProcessSupplier(String format, DependencyStorage dependencyStorage,
			String spanTopic) {
		this.dependencyStorage = dependencyStorage;
		this.spanTopic = spanTopic;
		this.spanBytesDecoder = SpanBytesDecoder.valueOf(format);
		this.spanSerde = new SpanSerde(format);
		this.spansSerde = new SpansSerde(format);
		this.dependencyLinkSerde = new DependencyLinkSerde();
	}

	public StreamProcessSupplier(DependencyStorage dependencyStorage, String spanTopic) {
		this(SpanBytesDecoder.JSON_V2.name(), dependencyStorage, spanTopic);
	}

	public Topology build() {
		var builder = new StreamsBuilder();
		builder.stream(spanTopic, Consumed.with(Serdes.String(), Serdes.String()))
				.mapValues(value -> spanBytesDecoder.decodeList(value.getBytes(UTF_8)))
				.flatMapValues(decodedSpans -> decodedSpans)
				.groupBy((none, span) -> span.traceId(),
						Serialized.with(Serdes.String(), spanSerde))
				.aggregate(ArrayList::new,
						(String traceId, Span span, List<Span> spans) -> {
							spans.add(span);
							return spans;
						}, Materialized.with(Serdes.String(), spansSerde))
				.toStream().filterNot((traceId, spans) -> spans.isEmpty())
				.mapValues(
						spans -> new DependencyLinker().putTrace(spans.iterator()).link())
				.flatMapValues(dependencyLinks -> dependencyLinks)
				.groupBy(
						(traceId, dependencyLink) -> String.format(
								DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
								dependencyLink.child()),
						Serialized.with(Serdes.String(), dependencyLinkSerde))
				.reduce((l, r) -> r,
						Materialized.with(Serdes.String(), dependencyLinkSerde))
				.toStream().foreach((dependencyPair, dependencyLink) -> dependencyStorage
						.put(LocalDate.now().toEpochDay(), dependencyLink));
		return builder.build();
	}

}