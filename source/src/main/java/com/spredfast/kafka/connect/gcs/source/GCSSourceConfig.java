package com.spredfast.kafka.connect.gcs.source;

import java.util.regex.Pattern;

public class GCSSourceConfig {
	public final String bucket;
	public String keyPrefix = "";
	public int pageSize = 500;
	public String startMarker = null; // for partial replay
	public Pattern keyPattern = GCSFilesReader.DEFAULT_PATTERN;
	public GCSFilesReader.InputFilter inputFilter = GCSFilesReader.InputFilter.GUNZIP;
	public GCSFilesReader.PartitionFilter partitionFilter = GCSFilesReader.PartitionFilter.MATCH_ALL;

	public GCSSourceConfig(String bucket) {
		this.bucket = bucket;
	}

	public GCSSourceConfig(String bucket, String keyPrefix, int pageSize, String startMarker, Pattern keyPattern, GCSFilesReader.InputFilter inputFilter, GCSFilesReader.PartitionFilter partitionFilter) {
		this.bucket = bucket;
		this.keyPrefix = keyPrefix;
		this.pageSize = pageSize;
		this.startMarker = startMarker;
		this.keyPattern = keyPattern;
		if (inputFilter != null) {
			this.inputFilter = inputFilter;
		}
		if (partitionFilter != null) {
			this.partitionFilter = partitionFilter;
		}
	}
}
