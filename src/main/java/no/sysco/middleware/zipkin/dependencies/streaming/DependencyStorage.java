package no.sysco.middleware.zipkin.dependencies.streaming;

import zipkin2.DependencyLink;

public interface DependencyStorage {
    void put(Long start, DependencyLink dependencyLink);

    void close();
}
