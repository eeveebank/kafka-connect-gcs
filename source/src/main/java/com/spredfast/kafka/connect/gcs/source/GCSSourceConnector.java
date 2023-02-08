package com.spredfast.kafka.connect.gcs.source;

import com.spredfast.kafka.connect.gcs.Constants;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class GCSSourceConnector extends SourceConnector {

	private static final int DEFAULT_PARTITION_COUNT = 200;
	private static final String MAX_PARTITION_COUNT = "max.partition.count";
	private Map<String, String> config;

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> config) {
		this.config = config;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return GCSSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int taskCount) {
		int maxNbOfPartitions = Optional.ofNullable(config.get(MAX_PARTITION_COUNT))
			.map(Integer::parseInt).orElse(DEFAULT_PARTITION_COUNT);

		int[] idx = new int[] { 0 };

		Boolean splitTopicsAcrossTasks = Boolean.parseBoolean(config.get("tasks.splitTopics"));

		int iter = splitTopicsAcrossTasks ? 1 : taskCount;

		return IntStream.range(0, taskCount).mapToObj(taskNum ->
			// each task gets every nth partition
			IntStream.iterate(splitTopicsAcrossTasks ? 0 : taskNum, i -> i + iter)
			.mapToObj(Integer::toString)
			.limit(maxNbOfPartitions / iter + 1).collect(joining(",")))
			.map(parts -> {
				Map<String, String> task = new HashMap<>(config);
				task.put("partitions", parts);
				task.put("taskNum", Integer.toString(idx[0]++));
				task.put("taskCount", Integer.toString(taskCount));
				return task;
			})
			.collect(toList());
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		// TODO
		return new ConfigDef();
	}
}
