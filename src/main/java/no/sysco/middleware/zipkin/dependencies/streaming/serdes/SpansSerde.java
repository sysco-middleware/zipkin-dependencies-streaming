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

	private final SpanBytesDecoder spanBytesDecoder;

	private final SpanBytesEncoder spanBytesEncoder;

	public SpansSerde(String format) {
		this.spanBytesDecoder = SpanBytesDecoder.valueOf(format);
		this.spanBytesEncoder = SpanBytesEncoder.valueOf(format);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Nothing to configure
	}

	@Override
	public void close() {
		// No resources to close
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
			// Nothing to configure
		}

		@Override
		public byte[] serialize(String topic, List<Span> data) {
			return spanBytesEncoder.encodeList(data);
		}

		@Override
		public void close() {
			// No resources to close
		}

	}

	private class SpansDeserializer implements Deserializer<List<Span>> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// Nothing to configure
		}

		@Override
		public List<Span> deserialize(String topic, byte[] data) {
			return spanBytesDecoder.decodeList(data);
		}

		@Override
		public void close() {
			// No resources to close
		}

	}

}
