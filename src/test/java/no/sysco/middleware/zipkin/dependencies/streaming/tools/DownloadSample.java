package no.sysco.middleware.zipkin.dependencies.streaming.tools;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;

public class DownloadSample {

	public static void main(String[] args) throws IOException {
		final var url = new URL(
				"https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-ui/testdata/messaging-kafka.json");
		final var readableByteChannel = Channels.newChannel(url.openStream());
		final var fileOutputStream = new FileOutputStream("spans.json");
		fileOutputStream.getChannel().transferFrom(readableByteChannel, 0,
				Long.MAX_VALUE);
	}

}
