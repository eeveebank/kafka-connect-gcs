package com.spredfast.kafka.connect.gcs;

//import com.amazonaws.client.builder.AwsClientBuilder;
//import com.amazonaws.services.gcs.AmazonGCS;
//import com.amazonaws.services.gcs.AmazonS3ClientBuilder;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.Map;
import java.util.Objects;

//import static com.amazonaws.services.s3.AmazonS3Client.S3_SERVICE_NAME;
//import static com.amazonaws.util.AwsHostNameUtils.parseRegion;
import static java.lang.Boolean.parseBoolean;

public class GCS {

	public static Storage gcsclient(Map<String, String> config) {
		// Use default credentials provider that looks in Env + Java properties + profile + instance role

		StorageOptions.Builder builder = StorageOptions.newBuilder();

		setGCSEndpoint(config, builder);
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

}
