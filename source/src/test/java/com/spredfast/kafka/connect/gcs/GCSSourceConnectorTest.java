package com.spredfast.kafka.connect.gcs;

import com.spredfast.kafka.connect.gcs.source.GCSFilesReader;
import com.spredfast.kafka.connect.gcs.source.GCSSourceConfig;
import com.spredfast.kafka.connect.gcs.source.GCSSourceConnector;
import com.spredfast.kafka.connect.gcs.source.GCSSourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GCSSourceConnectorTest {

	GCSSourceConnector connector;

	void setUp(Map<String, String> config) {
		connector = new GCSSourceConnector();
		connector.start(config);
	}

	Map<String, String> overrideConfig(Map<String, String> configOverrides) {
		Map<String, String> taskConfig = new HashMap<>();
		for (String key : configOverrides.keySet()) {
			taskConfig.put(key, configOverrides.get(key));
		}
		return taskConfig;
	}

	void withConfig(Map<String, String> configOverrides) {
		Map<String, String> taskConfig = overrideConfig(configOverrides);
		setUp(taskConfig);
	}

	@Test
	void testConfigSingleTask() {
		Map<String, String> configOverrides = new HashMap<>();
		//configOverrides.put("partitions", "1,999,2");
		withConfig(configOverrides);
		int taskCount = 1;
		List<Map<String, String>>  taskConfig = connector.taskConfigs(taskCount);
		assertEquals(1, taskConfig.size());
		String[] partitions = taskConfig.get(0).get("partitions").split(",");
		assertEquals(200/taskCount +1, partitions.length);
		assertEquals("0", Arrays.stream(partitions).findFirst().get());
		assertEquals(true, Arrays.stream(partitions).anyMatch(n -> n.equals("5")));
		assertEquals(true, Arrays.stream(partitions).anyMatch(n -> n.equals("1")));
	}

	@Test
	void testConfigMultiTasks() {
		Map<String, String> configOverrides = new HashMap<>();
		//configOverrides.put("partitions", "1,999,2");
		withConfig(configOverrides);
		int taskCount = 5;
		List<Map<String, String>>  taskConfig = connector.taskConfigs(5);
		assertEquals(5, taskConfig.size());
		String[] partitions = taskConfig.get(0).get("partitions").split(",");
		assertEquals(200/taskCount +1, partitions.length);
		assertEquals("0", Arrays.stream(partitions).findFirst().get());
		assertEquals(true, Arrays.stream(partitions).anyMatch(n -> n.equals("5")));
		assertEquals(false, Arrays.stream(partitions).anyMatch(n -> n.equals("1")));
	}



}
