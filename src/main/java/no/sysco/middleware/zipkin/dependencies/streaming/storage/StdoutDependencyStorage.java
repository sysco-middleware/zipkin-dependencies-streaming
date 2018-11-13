package no.sysco.middleware.zipkin.dependencies.streaming.storage;

import no.sysco.middleware.zipkin.dependencies.streaming.DependencyStorage;
import zipkin2.DependencyLink;

public class StdoutDependencyStorage implements DependencyStorage {

	@Override
	public void put(Long start, DependencyLink dependencyLink) {
		System.out.printf("%s=>%s%n", start, dependencyLink);
	}

	@Override
	public void close() {
		// Do nothing
	}

}
