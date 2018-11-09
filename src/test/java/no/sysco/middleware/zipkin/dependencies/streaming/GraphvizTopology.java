package no.sysco.middleware.zipkin.dependencies.streaming;

import no.sysco.middleware.kafka.util.StreamsTopologyGraphviz;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GraphvizTopology {

	public static void main(String[] args) {
		try {
			var path = Paths.get("docs/topology.puml");
			if (Files.exists(path)) {
				Files.delete(path);
			}
			var file = Files.createFile(path);
			var buffer = Files.newBufferedWriter(file);
			buffer.append("@startuml\n");
			buffer.newLine();
			buffer.write(StreamsTopologyGraphviz
					.print(new StreamProcessSupplier().build())
					// Support plantuml comment line
					.replace("#", "'"));
			buffer.newLine();
			buffer.append("@enduml");
			buffer.flush();
			buffer.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
