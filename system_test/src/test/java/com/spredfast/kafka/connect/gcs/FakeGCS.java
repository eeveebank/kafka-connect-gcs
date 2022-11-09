package com.spredfast.kafka.connect.gcs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

// https://github.com/fsouza/fake-gcs-server/blob/main/examples/java/src/test/java/com/fsouza/fakegcsserver/java/examples/FakeGcsServerTest.java
// https://stackoverflow.com/questions/70972595/getting-java-net-connectexception-connection-refused-connection-refused-while
// https://github.com/fsouza/fake-gcs-server/blob/main/examples/java/README.md
@Testcontainers
public class FakeGCS {
	Storage storageClient;
	@Container
	public GenericContainer<?> gcs = new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server"))
		.withExposedPorts(4443)
		.withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(
			"/bin/fake-gcs-server",
			"-scheme", "http"
		));

	public Storage startAndReturnClient(String bucketName) throws Exception {
		gcs.start();
		String fakeGcsExternalUrl = getEndpoint();
		updateExternalUrlWithContainerUrl(fakeGcsExternalUrl);

		storageClient = StorageOptions.newBuilder()
			.setHost(fakeGcsExternalUrl)
			.setProjectId("test-project")
			.setCredentials(NoCredentials.getInstance())
			.build()
			.getService();

		createBucket(bucketName);

		return storageClient;
	}

	public Bucket createBucket(String bucketName) {
		return storageClient.create(BucketInfo.newBuilder(bucketName).build());
	}

	public void close() {
		//gcs.close();
	}

	public String getEndpoint() {
		return String.format("http://%s:%s", gcs.getHost(), gcs.getFirstMappedPort());
	}

	private static void updateExternalUrlWithContainerUrl(String fakeGcsExternalUrl) throws Exception {
		String modifyExternalUrlRequestUri = fakeGcsExternalUrl + "/_internal/config";
		String updateExternalUrlJson = "{"
			+ "\"externalUrl\": \"" + fakeGcsExternalUrl + "\""
			+ "}";

		HttpRequest req = HttpRequest.newBuilder()
			.uri(URI.create(modifyExternalUrlRequestUri))
			.header("Content-Type", "application/json")
			.PUT(HttpRequest.BodyPublishers.ofString(updateExternalUrlJson))
			.build();
		HttpResponse<Void> response = HttpClient.newBuilder().build()
			.send(req, HttpResponse.BodyHandlers.discarding());

		if (response.statusCode() != 200) {
			throw new RuntimeException(
				"error updating fake-gcs-server with external url, response status code " + response.statusCode() + " != 200");
		}
	}
}
