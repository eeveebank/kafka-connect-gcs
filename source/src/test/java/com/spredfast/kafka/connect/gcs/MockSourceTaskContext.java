package com.spredfast.kafka.connect.gcs;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class MockSourceTaskContext implements SourceTaskContext {
	public Map<String,String> configs() {
		return new HashMap<>();
	}

	private String bucketName;

	public MockSourceTaskContext(String theBucketName) {
		bucketName = theBucketName;
	};

	public OffsetStorageReader offsetStorageReader() {
		return new MockOffsetStorageReader(bucketName);
	}
}
