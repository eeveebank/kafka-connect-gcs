package com.spredfast.kafka.connect.gcs.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GCSPartition {

	private final String bucket;
	private final String keyPrefix;
	private final String topic;
	private final int partition;

	public GCSPartition(String bucket, String keyPrefix, String topic, int partition) {
		this.bucket = bucket;
		this.keyPrefix = normalizePrefix(keyPrefix);
		this.topic = topic;
		this.partition = partition;
	}

	public static GCSPartition from(String bucket, String keyPrefix, String topic, int partition) {
		return new GCSPartition(bucket, keyPrefix, topic, partition);
	}

	public static GCSPartition from(Map<String, Object> map) {
		String bucket = (String) map.get("bucket");
		String keyPrefix = (String) map.get("keyPrefix");
		String topic = (String) map.get("topic");
		int partition;
		if (map.get("kafkaPartition").getClass().getSimpleName().equals("String")) {
			// just for testing..
			partition = Integer.parseInt((String)map.get("kafkaPartition"));
		} else {
			partition = ((Number) map.get("kafkaPartition")).intValue();
		}
		return from(bucket, keyPrefix, topic, partition);
	}

	public static String normalizePrefix(String keyPrefix) {
		return keyPrefix == null ? ""
			: keyPrefix.endsWith("/") ? keyPrefix : keyPrefix + "/";
	}

	public Map<String, Object> asMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("bucket", bucket);
		map.put("keyPrefix", keyPrefix);
		map.put("topic", topic);
		map.put("kafkaPartition", partition);
		return map;
	}

	public String getBucket() {
		return bucket;
	}

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public String getTopic() {
		return topic;
	}

	public int getPartition() {
		return partition;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GCSPartition that = (GCSPartition) o;
		return partition == that.partition &&
			Objects.equals(bucket, that.bucket) &&
			Objects.equals(keyPrefix, that.keyPrefix) &&
			Objects.equals(topic, that.topic);
	}

	@Override
	public int hashCode() {
		return Objects.hash(bucket, keyPrefix, topic, partition);
	}

	@Override
	public String toString() {
		return bucket + "/" + keyPrefix + "/" + topic  + "-" + partition;
	}
}
