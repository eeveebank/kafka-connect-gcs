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

	public OffsetStorageReader offsetStorageReader() {
		return new OffsetStorageReader() {
			public <T> Map<String, Object> offset(Map<String, T> partition) {
				return new HashMap<>();
			}

			public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
				return new HashMap<>();
			}
		};
	}
}
