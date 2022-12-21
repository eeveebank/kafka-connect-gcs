package com.spredfast.kafka.connect.gcs;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class MockOffsetStorageReader implements OffsetStorageReader {

	private String bucketName;

	public MockOffsetStorageReader(String theBucketName) {
		bucketName = theBucketName;
	};

	public <T> Map<String, Object> offset(Map<String, T> partition) {
		Map<String, Object> offset = new HashMap<>();
		return offset;
	}

	public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
		Map<Map<String, T>, Map<String, Object>> offsets = new HashMap<>();
		Map<String, T> key = new HashMap<>();
		key.put("bucket", (T)bucketName);
		key.put("keyPrefix", (T)"");
		key.put("topic", (T)"1");
		Object nb = "2";
		key.put("kafkaPartition", (T)nb);

		Map<String, Object> value = new HashMap<>();
		value.put("gcskey", "dummyKey1");
		value.put("originalOffset", Long.valueOf(100));
		offsets.put(key, value);
		return offsets;
	}

}

