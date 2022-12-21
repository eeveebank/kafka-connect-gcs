package com.spredfast.kafka.connect.gcs;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class MockOffsetStorageReader implements OffsetStorageReader {

	private String bucketName;
	private int offset;

	public MockOffsetStorageReader(String theBucketName) {
		bucketName = theBucketName;
		offset = 199;
	};

	public <T> Map<String, Object> offset(Map<String, T> partition) {
		Map<String, Object> offset = new HashMap<>();
		return offset;
	}

	public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
		Map<Map<String, T>, Map<String, Object>> offsets = new HashMap<>();
		for (Map<String, T> partition : partitions) {
			Map<String, T> key = new HashMap<>();
			key.put("bucket", (T)bucketName);
			key.put("keyPrefix", partition.get("keyPrefix"));
			key.put("topic", partition.get("topic"));
			key.put("kafkaPartition", partition.get("kafkaPartition"));
			Map<String, Object> value = new HashMap<>();
			value.put("gcskey", "dummyKey1");
			value.put("originalOffset", Long.valueOf(offset));
			offsets.put(key, value);
		}

		return offsets;
	}

}

