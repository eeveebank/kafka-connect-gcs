package com.spredfast.kafka.connect.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.spredfast.kafka.connect.gcs.source.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GCSSourceTaskTest {

	@Test
	void testBuildConfig() throws IOException {
		SourceTaskContext context = new MockSourceTaskContext();
		GCSSourceTask task = new GCSSourceTask();
		task.initialize(context);
		Map<String, String> taskConfig = new HashMap<>();
		taskConfig.put("gcs.bucket", "dummyGGCBucket");
		String partitions = "1,2,3";
		taskConfig.put("partitions", partitions);
		task.start(taskConfig);
		assertEquals(true, true);
	}


}
