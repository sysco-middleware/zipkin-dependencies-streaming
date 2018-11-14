package no.sysco.middleware.zipkin.dependencies.streaming.tools;

import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.kafka11.KafkaSender;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class SpanKafkaProducer {

	public static void main(String[] args) throws IOException, InterruptedException {
		final var bytes = Files.readAllBytes(Paths.get("spans.json"));
		final var list = SpanBytesDecoder.JSON_V2.decodeList(bytes);
		final var sender = KafkaSender.create("localhost:19092");
		sender.sendSpans(list.stream().map(SpanBytesEncoder.JSON_V2::encode)
				.collect(Collectors.toList())).execute();

		Thread.sleep(10_000);
	}

}
