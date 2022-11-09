package com.spredfast.kafka.connect.gcs.source;

import org.apache.kafka.connect.header.Headers;

public class GCSSourceRecord {
	private final GCSPartition file;
	private final GCSOffset offset;
	private final String topic;
	private final int partition;
	private final byte[] key;
	private final byte[] value;
	private final Headers headers;


	public GCSSourceRecord(GCSPartition file, GCSOffset offset, String topic, int partition, byte[] key, byte[] value, final Headers headers) {
		this.file = file;
		this.offset = offset;
		this.topic = topic;
		this.partition = partition;
		this.key = key;
		this.value = value;
		this.headers = headers;
	}

	public GCSPartition file() {
		return file;
	}

	public GCSOffset offset() {
		return offset;
	}

	public String topic() {
		return topic;
	}

	public int partition() {
		return partition;
	}

	public byte[] key() {
		return key;
	}

	public byte[] value() {
		return value;
	}

	public Headers headers() {
		return headers;
	}
}
