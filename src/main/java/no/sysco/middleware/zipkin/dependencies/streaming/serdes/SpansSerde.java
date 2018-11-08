package no.sysco.middleware.zipkin.dependencies.streaming.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import java.util.List;
import java.util.Map;

public class SpansSerde implements Serde<List<Span>> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<List<Span>> serializer() {
		return new SpansSerializer();
	}

	@Override
	public Deserializer<List<Span>> deserializer() {
		return new SpansDeserializer();
	}

	private class SpansSerializer implements Serializer<List<Span>> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public byte[] serialize(String topic, List<Span> data) {
			return SpanBytesEncoder.JSON_V2.encodeList(data);
		}

		@Override
		public void close() {
		}

	}

	private class SpansDeserializer implements Deserializer<List<Span>> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public List<Span> deserialize(String topic, byte[] data) {
			return SpanBytesDecoder.JSON_V2.decodeList(data);
		}

		@Override
		public void close() {
		}

	}

}
