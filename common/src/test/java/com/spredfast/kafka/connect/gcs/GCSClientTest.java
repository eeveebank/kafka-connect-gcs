package com.spredfast.kafka.connect.gcs;

import com.google.cloud.storage.Storage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

public class GCSClientTest {
	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Test
	public void shouldConstructClient() {
		// set the AWS region for test purposes
		environmentVariables.set("AWS_REGION", "eu-west-2");
		Map<String, String> config = new HashMap<>();
		config.put("gcs.endpoint", "https://gcs-eu-west-2.amazonaws.com");
		Storage client = GCS.gcsclient(config);
		assertThat(client, not(nullValue()));
	}
}
