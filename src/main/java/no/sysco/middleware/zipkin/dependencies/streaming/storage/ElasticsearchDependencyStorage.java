package no.sysco.middleware.zipkin.dependencies.streaming.storage;

import no.sysco.middleware.zipkin.dependencies.streaming.DependencyStorage;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.DependencyLink;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;

import static java.time.ZoneOffset.UTC;

public class ElasticsearchDependencyStorage implements DependencyStorage {

	static private final Logger LOGGER = LoggerFactory
			.getLogger(CassandraDependencyStorage.class.getName());

	static private final String indexPattern = "%s:dependency-%s";

	private final RestHighLevelClient restHighLevelClient;

	private final String index;

	private final String dateSeparator;

	public ElasticsearchDependencyStorage(RestHighLevelClient restHighLevelClient,
			String index, String dateSeparator) {
		this.restHighLevelClient = restHighLevelClient;
		this.index = index;
		this.dateSeparator = dateSeparator;
	}

	@Override
	public void put(Long start, DependencyLink dependencyLink) {
		try {
			final var dateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(start),
					UTC);
			final var dateTimeFormatter = DateTimeFormatter
					.ofPattern("yyyy-MM-dd".replace("-", dateSeparator));
			final var dependencyIndex = String.format(indexPattern, index,
					dateTime.format(dateTimeFormatter));

			final var result = new LinkedHashMap<String, Object>();
			result.put("id", dependencyLink.parent() + "|" + dependencyLink.child());
			result.put("parent", dependencyLink.parent());
			result.put("child", dependencyLink.child());
			result.put("callCount", dependencyLink.callCount());
			result.put("errorCount", dependencyLink.errorCount());

			restHighLevelClient.index(
					new IndexRequest(dependencyIndex, "dependency").source(result),
					RequestOptions.DEFAULT);
			LOGGER.info("DependencyLink stored: {}", dependencyLink);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			restHighLevelClient.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
