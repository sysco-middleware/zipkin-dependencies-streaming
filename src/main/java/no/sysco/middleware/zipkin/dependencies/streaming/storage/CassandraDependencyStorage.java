package no.sysco.middleware.zipkin.dependencies.streaming.storage;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import no.sysco.middleware.zipkin.dependencies.streaming.DependencyStorage;
import zipkin2.DependencyLink;

public class CassandraDependencyStorage implements DependencyStorage {

	private final Session session;

	private final PreparedStatement prepared;

	public CassandraDependencyStorage(String keyspace, String[] addresses) {
		final var cluster = Cluster.builder().addContactPoints(addresses).build();
		this.session = cluster.connect();
		this.prepared = session.prepare(QueryBuilder.insertInto(keyspace, "dependency")
				.value("day", QueryBuilder.bindMarker("day"))
				.value("parent", QueryBuilder.bindMarker("parent"))
				.value("child", QueryBuilder.bindMarker("child"))
				.value("calls", QueryBuilder.bindMarker("calls"))
				.value("errors", QueryBuilder.bindMarker("errors")));
	}

	@Override
	public void put(Long start, DependencyLink dependencyLink) {
		final var bound = prepared.bind()
				.setDate("day", LocalDate.fromMillisSinceEpoch(start))
				.setString("parent", dependencyLink.parent())
				.setString("child", dependencyLink.child())
				.setLong("calls", dependencyLink.callCount());
		if (dependencyLink.errorCount() > 0L) {
			bound.setLong("errors", dependencyLink.errorCount());
		}
		session.execute(bound);
	}

	@Override
	public void close() {
		session.close();
	}

}
