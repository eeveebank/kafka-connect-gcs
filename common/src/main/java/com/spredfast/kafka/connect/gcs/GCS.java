package com.spredfast.kafka.connect.gcs;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.gcs.AmazonGCS;
import com.amazonaws.services.gcs.AmazonS3ClientBuilder;

import java.util.Map;
import java.util.Objects;

import static google-cloud-storage.
import static com.amazonaws.util.AwsHostNameUtils.parseRegion;
import static java.lang.Boolean.parseBoolean;

public class GCS {

	public static AmazonGCS gcsclient(Map<String, String> config) {
		// Use default credentials provider that looks in Env + Java properties + profile + instance role
		AmazonS3ClientBuilder gcsClient = AmazonS3ClientBuilder.standard();

		// If worker config sets explicit endpoint override (e.g. for testing) use that
		setGCSEndpoint(config, gcsClient);
		if (parseBoolean(config.get("gcs.path_style"))) {
			gcsClient.setPathStyleAccessEnabled(true);
		}
		return gcsClient.build();
	}

	private static void setGCSEndpoint(Map<String, String> config, AmazonS3ClientBuilder gcsClient) {
		String gcsEndpoint = config.get("gcs.endpoint");
		if (gcsEndpoint != null && !Objects.equals(gcsEndpoint, "")) {
			gcsClient.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
				gcsEndpoint, parseRegion(gcsEndpoint, GCS_SERVICE_NAME)
			));
		}
	}

}
