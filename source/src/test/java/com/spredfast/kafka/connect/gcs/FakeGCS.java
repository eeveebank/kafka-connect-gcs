package com.spredfast.kafka.connect.gcs;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

// https://github.com/fsouza/fake-gcs-server/blob/main/examples/java/src/test/java/com/fsouza/fakegcsserver/java/examples/FakeGcsServerTest.java
@Testcontainers
public class FakeGCS {

	@Container
	public GenericContainer gcs = new GenericContainer(DockerImageName.parse("fsouza/fake-gcs-server"))
		.withExposedPorts(9090);

	public void start() {
		gcs.start();
	}

	public void close() {
		gcs.close();
	}

	public String getEndpoint() {
		return String.format("http://%s:%s", gcs.getHost(), gcs.getFirstMappedPort());
	}
}
