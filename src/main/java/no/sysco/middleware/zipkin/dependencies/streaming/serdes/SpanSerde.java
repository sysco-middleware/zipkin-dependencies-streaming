package no.sysco.middleware.zipkin.dependencies.streaming.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import java.util.Map;

public class SpanSerde implements Serde<Span> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<Span> serializer() {
		return new SpanSerializer();
	}

	@Override
	public Deserializer<Span> deserializer() {
		return new SpanDeserializer();
	}

	private class SpanSerializer implements Serializer<Span> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public byte[] serialize(String topic, Span data) {
			return SpanBytesEncoder.JSON_V2.encode(data);
		}

		@Override
		public void close() {
		}

	}

	private class SpanDeserializer implements Deserializer<Span> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public Span deserialize(String topic, byte[] data) {
			return SpanBytesDecoder.JSON_V2.decodeOne(data);
		}

		@Override
		public void close() {
		}

	}

}
