package com.spredfast.kafka.connect.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.spredfast.kafka.connect.gcs.source.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GCSSourceTaskTest {

	GCSFilesReader.PartitionFilter partitionFilter;

	String BUCKET_NAME = "dummyBucket";

	void setUp(Map<String, String> taskConfig) throws Exception {
		SourceTaskContext context = new MockSourceTaskContext(BUCKET_NAME);
		GCSSourceTask task = new GCSSourceTask();
		task.initialize(context);
		task.start(taskConfig);
		GCSSourceConfig gcsSourceConfig = task.gcsSourceConfig;
		partitionFilter = gcsSourceConfig.partitionFilter;
	}

	Map<String, String> overrideConfig(Map<String, String> configOverrides) {
		Map<String, String> taskConfig = new HashMap<>();
		taskConfig.put("gcs.bucket", BUCKET_NAME);
		taskConfig.put("partitions", "0,1,2");
		taskConfig.put("topic", "");
		for (String key : configOverrides.keySet()) {
			taskConfig.put(key, configOverrides.get(key));
		}
		return taskConfig;
	}

	void withConfig(Map<String, String> configOverrides) throws Exception {
		Map<String, String> taskConfig = overrideConfig(configOverrides);
		setUp(taskConfig);
	}


	@Test
	void testBuildConfigPartitionMatch() throws Exception {
		Map<String, String> configOverrides = new HashMap<>();
		configOverrides.put("partitions", "1,999,2");
		withConfig(configOverrides);
		Boolean match1 = partitionFilter.matches("anyTopic", 999);
		assertEquals(true, match1);
		Boolean match2 = partitionFilter.matches("anyTopic", 998);
		assertEquals(false, match2);
	}

	@Test
	void testBuildConfigAllTopicsMatch() throws Exception {
		Map<String, String> configOverrides = new HashMap<>();
		withConfig(configOverrides);
		Boolean match1 = partitionFilter.matches("anyTopic", 1);
		assertEquals(true, match1);
	}

	@Test
	void testBuildConfigSomeTopicsMatch() throws Exception {
		Map<String, String> configOverrides = new HashMap<>();
		configOverrides.put("topics", "topic1,topic2");
		withConfig(configOverrides);
		Boolean match1 = partitionFilter.matches("anyTopic", 1);
		assertEquals(false, match1);
		Boolean match2 = partitionFilter.matches("topic1", 1);
		assertEquals(true, match2);
		Boolean match3 = partitionFilter.matches("topic2", 1);
		assertEquals(true, match3);
	}

	@Test
	void testBuildConfigAllTopicsMatchSomeTopicsIgnored() throws Exception {
		Map<String, String> configOverrides = new HashMap<>();
		configOverrides.put("topics.ignore", "topic1,topic2");
		withConfig(configOverrides);
		Boolean match1 = partitionFilter.matches("anyTopic", 1);
		assertEquals(true, match1);
		Boolean match2 = partitionFilter.matches("topic1", 1);
		assertEquals(false, match2);
		Boolean match3 = partitionFilter.matches("topic2", 1);
		assertEquals(false, match3);
	}

	void checkThatOnlyThisTaskMatches(String topicName, int partition, String taskNumToMatch) throws Exception {
		Map<String, String> taskConfig = new HashMap<>();
		String[] taskNums = {"0", "1", "2", "3", "4"};
		for (int i = 0; i <= taskNums.length - 1; i++) {
			String taskNum = taskNums[i];
			taskConfig.put("taskNum", taskNum);
			taskConfig.put("tasks.splitTopics", "true");
			taskConfig.put("taskCount", "5");
			withConfig(taskConfig);
			assertEquals(partitionFilter.matches(topicName, partition), taskNum.equals(taskNumToMatch) );
		}
	}

	@Test
	void testBuildConfigSplitTopicsAcrossTasks() throws Exception {
		// it doesn't matter much which value for taskNumToMatch happen to work here - should be one between 0 and 4
		// but checkThatOnlyThisTaskMatches checks that it works and that 's the only value working
		// also using different topicName / partition combinations here help seeing that the partitions are roughly balanced
		String topicName = "eevee-banking-1";
		checkThatOnlyThisTaskMatches(topicName,0, "1");

		topicName = "eevee-banking-2";
		checkThatOnlyThisTaskMatches(topicName,0, "0");

		topicName = "eevee-adapter";
		checkThatOnlyThisTaskMatches(topicName,0, "0");

		topicName = "eevee-albatros";
		checkThatOnlyThisTaskMatches(topicName,0, "4");
		checkThatOnlyThisTaskMatches(topicName,1, "2");
		checkThatOnlyThisTaskMatches(topicName,2, "1");
		checkThatOnlyThisTaskMatches(topicName,3, "1");

	}

	private void write(BlockGZIPFileWriter writer, byte[] key, byte[] value, Headers headers, boolean includeKeys) throws IOException {
		writer.write(new ByteLengthFormat(includeKeys).newWriter().writeBatch(Stream.of(
			new ProducerRecord<>("", 0, key, value, headers)
		)).collect(toList()), 1);
	}

	private static final BiFunction<String, String, Headers> headersFunction = (k, v) -> new RecordHeaders(new RecordHeader[]{new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))});

	private void uploadToGCS(Storage storage, Path dir) throws IOException {
		// Optional: set a generation-match precondition to avoid potential race
		// conditions and data corruptions. The request returns a 412 error if the
		// preconditions are not met.
		// For a target object that does not yet exist, set the DoesNotExist precondition.
		Storage.BlobTargetOption precondition = Storage.BlobTargetOption.doesNotExist();

		Files.walk(dir).filter(Files::isRegularFile).forEach(f -> {
			Path relative = dir.relativize(f);
			System.out.println("Writing " + relative.toString());
			// https://cloud.google.com/storage/docs/samples/storage-upload-file
			BlobId blobId = BlobId.of(BUCKET_NAME, relative.toString());
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
			try {
				storage.create(blobInfo, Files.readAllBytes(f), precondition);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private void givenASingleDayWithManyPartitions(Storage client, Path dir, boolean includeKeys) throws IOException {
		new File(dir.toFile(), "prefix/2016-01-01").mkdirs();
		try (BlockGZIPFileWriter p0 = new BlockGZIPFileWriter("topic-00000", dir.toString() + "/prefix/2016-01-01", 0, 512);
			 BlockGZIPFileWriter p1 = new BlockGZIPFileWriter("topic-00001", dir.toString() + "/prefix/2016-01-01", 0, 512);
		) {
			write(p0, "key0-0".getBytes(), "value0-0".getBytes(), headersFunction.apply("header key 0-0", "header value 0-0"), includeKeys);
			write(p1, "key1-0".getBytes(), "value1-0".getBytes(), headersFunction.apply("header key 1-0", "header value 1-0"), includeKeys);
			write(p1, "key1-1".getBytes(), "value1-1".getBytes(), headersFunction.apply("header key 1-1", "header value 1-1"), includeKeys);
		}
		uploadToGCS(client, dir);
	}

	@Test
	void testTryReadFromOffsets() throws Exception {
		FakeGCS gcs = new FakeGCS();;
		Storage	storageClient = gcs.startAndReturnClient(BUCKET_NAME);
		SourceTaskContext context = new MockSourceTaskContext(BUCKET_NAME);
		GCSSourceTask task = new GCSSourceTask();
		task.initialize(context);
		task.gcsClient = storageClient;

		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenASingleDayWithManyPartitions(storageClient, dir, true);

		Map<String, String> taskConfig = new HashMap<>();
		taskConfig.put("gcs.bucket", BUCKET_NAME);
		String prefix = "prefix";
		taskConfig.put("gcs.prefix", prefix);
		int partition = 1;
		taskConfig.put("partitions", Integer.toString(partition));
		String topics = "topic1,topic2";
		taskConfig.put("topics", topics);
		task.start(taskConfig);
		int offset = 199; // see MockOffsetStorageReader
		assertEquals(task.offsets.keySet().size(), 2);
		for (GCSPartition gcsPartition : task.offsets.keySet()) {
			assertEquals(BUCKET_NAME, gcsPartition.getBucket());
			assertEquals(prefix +"/", gcsPartition.getKeyPrefix());
			assertEquals(partition, gcsPartition.getPartition());
			assertTrue(topics.contains(gcsPartition.getTopic()));
			GCSOffset gcsOffset = task.offsets.get(gcsPartition);
			assertEquals(offset, gcsOffset.getOffset());
		}
	}

	@Test
	void testTryReadFromOffsetsSplitTopicsAcrossTasks() throws Exception {
		FakeGCS gcs = new FakeGCS();;
		Storage	storageClient = gcs.startAndReturnClient(BUCKET_NAME);
		SourceTaskContext context = new MockSourceTaskContext(BUCKET_NAME);
		GCSSourceTask task = new GCSSourceTask();
		task.initialize(context);
		task.gcsClient = storageClient;

		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenASingleDayWithManyPartitions(storageClient, dir, true);

		Map<String, String> taskConfig = new HashMap<>();
		taskConfig.put("gcs.bucket", BUCKET_NAME);
		String prefix = "prefix";
		taskConfig.put("gcs.prefix", prefix);
		taskConfig.put("tasks.splitTopics", "true");
		taskConfig.put("taskNum", "1");
		taskConfig.put("taskCount", "2");
		int partition = 1;
		taskConfig.put("partitions", Integer.toString(partition));
		String topics = "topic1,topic2";
		taskConfig.put("topics", topics);
		task.start(taskConfig);
		int offset = 199; // see MockOffsetStorageReader
		assertEquals(task.offsets.keySet().size(), 1);
		for (GCSPartition gcsPartition : task.offsets.keySet()) {
			assertEquals(BUCKET_NAME, gcsPartition.getBucket());
			assertEquals(prefix +"/", gcsPartition.getKeyPrefix());
			assertEquals(partition, gcsPartition.getPartition());
			assertTrue(topics.contains(gcsPartition.getTopic()));
			GCSOffset gcsOffset = task.offsets.get(gcsPartition);
			assertEquals(offset, gcsOffset.getOffset());
		}
	}

}
