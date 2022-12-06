package com.spredfast.kafka.connect.gcs;

//import com.amazonaws.client.builder.AwsClientBuilder;
//import com.amazonaws.services.gcs.AmazonGCS;
//import com.amazonaws.services.gcs.AmazonS3ClientBuilder;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.Map;
import java.util.Objects;

import static java.lang.Boolean.parseBoolean;

public class GCS {

	public static Storage gcsclient(Map<String, String> config) {
		// https://cloud.google.com/docs/authentication/client-libraries#example
		StorageOptions.Builder builder = StorageOptions.newBuilder();

		setGCSEndpoint(config, builder);
		setProjectId(config, builder);
		Storage storage = builder
			.build()
			.getService();

		return storage;
	}

	private static void setGCSEndpoint(Map<String, String> config, StorageOptions.Builder builder) {
		String gcsEndpoint = config.get("gcs.endpoint");
		if (gcsEndpoint != null && !Objects.equals(gcsEndpoint, "")) {
			builder.setHost(gcsEndpoint);
		}
	}

	private static void setProjectId(Map<String, String> config, StorageOptions.Builder builder) {
		String projectId = config.get("projectId");
		if (projectId != null && !Objects.equals(projectId, "")) {
			builder.setProjectId(projectId);
		}
	}

}
