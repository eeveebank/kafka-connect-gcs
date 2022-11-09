package com.spredfast.kafka.connect.gcs;

//import com.amazonaws.services.gcs.Storage;
//import com.amazonaws.services.gcs.model.GCSObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.spredfast.kafka.connect.gcs.json.ChunksIndex;
//import com.spredfast.kafka.connect.gcs.sink.GCSSinkConnector;
//import com.spredfast.kafka.connect.gcs.sink.GCSSinkTask;
import com.spredfast.kafka.connect.gcs.source.GCSFilesReader;
import com.spredfast.kafka.connect.gcs.source.GCSSourceConnector;
import com.spredfast.kafka.connect.gcs.source.GCSSourceTask;
import com.spredfast.kafka.test.KafkaIntegrationTests;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Maps.transformValues;
import static com.spredfast.kafka.test.KafkaIntegrationTests.givenKafkaConnect;
import static com.spredfast.kafka.test.KafkaIntegrationTests.givenLocalKafka;
import static com.spredfast.kafka.test.KafkaIntegrationTests.waitForPassing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.minBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GCSConnectorIntegrationTest {

	private static final String GCS_BUCKET = "connect-system-test";
	private static final String GCS_BUCKET2 = "connect-system-test2";
	private static final String GCS_BUCKET3 = "connect-system-test3";
	private static final String GCS_PREFIX = "binsystest";
	private static KafkaIntegrationTests.Kafka kafka;
	private static KafkaIntegrationTests.KafkaConnect connect;
	private static FakeGCS gcs;
	private static KafkaIntegrationTests.KafkaConnect stringConnect;

	@BeforeAll
	public static void startKafka() throws Exception {
		kafka = givenLocalKafka();
		connect = givenKafkaConnect(kafka.getLocalPort());
		stringConnect = givenKafkaConnect(kafka.getLocalPort(), ImmutableMap.of(
			"value.converter", StringConverter.class.getName(),
			"converter.type", ConverterType.VALUE.getName()
		));

		gcs = new FakeGCS();

//		ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
//		Logger.getRootLogger().addAppender(consoleAppender);
//		Logger.getRootLogger().setLevel(Level.ERROR);
//		//Logger.getLogger(GCSSinkTask.class).setLevel(Level.DEBUG);
//		Logger.getLogger(GCSSourceTask.class).setLevel(Level.DEBUG);
//		Logger.getLogger(GCSFilesReader.class).setLevel(Level.DEBUG);
//		Logger.getLogger(FileOffsetBackingStore.class).setLevel(Level.DEBUG);
	}

	@AfterAll
	public static void stopKafka() throws Exception {
		tryClose(connect);
		tryClose(stringConnect);
		tryClose(kafka);
		tryClose(() -> gcs.close());
	}

	@BeforeEach
	public void setup() {
		gcs = new FakeGCS();
	}

	@AfterEach
	public void cleanup() {
		// stop all the connectors
		connect.herder().connectors((e, connectors) ->
			connectors.forEach(this::whenTheSinkIsStopped));
		tryClose(() -> gcs.close());
	}

//	@Test
//	void binaryWithKeys() throws Exception {
//		// this gross, expensive initialization is necessary to ensure we are handling offsets correctly
//		// If we're not, the restart will produce duplicates in GCS, and files with incorrect offsets.
//		Map.Entry<String, List<Long>> topicAndOffsets = slow_givenATopicWithNonZeroStartOffsets();
//		String sinkTopic = topicAndOffsets.getKey();
//
//		Producer<String, String> producer = givenKafkaProducer();
//
//		givenRecords(sinkTopic, producer, 0);
//
//		Map<String, String> sinkConfig = givenSinkConfig(sinkTopic);
//		Storage storage = givenGCSClient(sinkConfig);
//		gcs.createBucket(GCS_BUCKET);
//		whenTheConnectorIsStarted(sinkConfig);
//
//		thenFilesAreWrittenToGCS(gcs.storageClient, sinkTopic, topicAndOffsets.getValue());
//
//		String sourceTopic = kafka.createUniqueTopic("bin-source-replay-", 2);
//
//		Map<String, String> sourceConfig = givenSourceConfig(sourceTopic, sinkTopic);
//		whenTheConnectorIsStarted(sourceConfig);
//
//		thenMessagesAreRestored(sourceTopic, gcs.storageClient);
//
//		whenConnectIsStopped();
//		givenRecords(sinkTopic, producer, 5);
//
//		whenConnectIsRestarted();
//		whenTheConnectorIsStarted(sinkConfig);
//		whenTheConnectorIsStarted(sourceConfig);
//
//		// This will fail if we get duplicates into the sink topic, which can happen because we have duplicates
//		// in GCS (source bug) or because of a bug in the source.
//		thenMoreMessagesAreRestored(sourceTopic, gcs.storageClient);
//
//		whenTheSinkIsStopped(sinkConfig.get("name"));
//
//		thenTempFilesAreCleanedUp(sinkConfig);
//	}

//	@Test
//	void binaryWithHeaders() throws Exception {
//		Map.Entry<String, List<Long>> topicAndOffsets = givenATopicWithZeroStartOffsets();
//		String sinkTopic = topicAndOffsets.getKey();
//
//		Producer<String, String> producer = givenKafkaProducer();
//
//		Function<Integer, Headers> headersFunction = (i) -> {
//			Headers headers = new RecordHeaders();
//			headers.add("headerWithEmptyValue", "".getBytes(StandardCharsets.UTF_8));
//			headers.add("headerWithNullValue", null);
//			headers.add("headerWithValue", ("headerValue:" + i).getBytes(StandardCharsets.UTF_8));
//			headers.add("", ("headerValueWithEmptyKey:" + i).getBytes(StandardCharsets.UTF_8));
//			return headers;
//		};
//
//		int numberOfMessages = 5;
//		List<TestRecord> expectedRecords = IntStream.range(0, numberOfMessages).mapToObj(i -> {
//				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(sinkTopic, 0, "key:" + i, "value:" + i, headersFunction.apply(i));
//				try {
//					producer.send(producerRecord).get(5, TimeUnit.SECONDS);
//				} catch (Exception e) {
//					throw Throwables.propagate(e);
//				}
//				return new TestRecord(producerRecord.key(), producerRecord.value(), producerRecord.headers());
//			}
//		).collect(Collectors.toList());
//
//
//		Map<String, String> sinkConfig = givenSinkConfig(sinkTopic);
//		Storage storage = givenGCSClient(sinkConfig);
//		gcs.createBucket(GCS_BUCKET);
//		whenTheConnectorIsStarted(sinkConfig);
//
//		String sourceTopic = kafka.createUniqueTopic("bin-source-replay-", 2);
//		Map<String, String> sourceConfig = givenSourceConfig(sourceTopic, sinkTopic);
//		whenTheConnectorIsStarted(sourceConfig);
//
//		List<TestRecord> records = consumeTopic(sourceTopic, numberOfMessages).stream()
//			.map(r -> new TestRecord(r.key(), r.value(), r.headers()))
//			.collect(toList());
//		assertThat(records).containsExactlyInAnyOrderElementsOf(expectedRecords);
//	}

//	@Test
//	void stringWithoutKeys() throws Exception {
//		String sinkTopic = kafka.createUniqueTopic("txt-sink-source-", 2);
//
//		Producer<String, String> producer = givenKafkaProducer();
//
//		givenRecords(sinkTopic, producer, 0);
//
//		Map<String, String> sinkConfig = givenStringWithoutKeysValues(givenSinkConfig(sinkTopic));
//		whenTheConnectorIsStarted(sinkConfig, stringConnect);
//
//		String sourceTopic = kafka.createUniqueTopic("txt-source-replay-", 2);
//
//		Map<String, String> sourceConfig = givenStringWithoutKeysValues(givenSourceConfig(sourceTopic, sinkTopic));
//		whenTheConnectorIsStarted(sourceConfig, stringConnect);
//
//		Storage storage = givenGCSClient(sinkConfig);
//		gcs.createBucket(GCS_BUCKET);
//
//		thenMessagesAreRestored(sourceTopic, gcs.storageClient);
//	}

	private Map<String, String> givenStringWithoutKeysValues(Map<String, String> config) {
		Map<String, String> copy = new HashMap<>(config);
		copy.remove("key.converter");
		copy.remove("format.include.keys");
		copy.remove("format");
		copy.putAll(ImmutableMap.of(
			"value.converter", StringConverter.class.getName(),
			"converter.type", ConverterType.VALUE.getName()
		));
		return copy;
	}

	private void whenConnectIsRestarted() {
		connect = connect.restart();
	}

	private void whenConnectIsStopped() throws Exception {
		connect.close();
	}


	private Consumer<String, String> givenAConsumer() {
		return new KafkaConsumer<>(ImmutableMap.of("bootstrap.servers", "localhost:" + kafka.getLocalPort()),
			new StringDeserializer(), new StringDeserializer());
	}

	private Storage givenGCSClient(Map<String, String> config) {
		return GCS.gcsclient(config);
	}

	private void whenTheConnectorIsStarted(Map<String, String> config) {
		whenTheConnectorIsStarted(config, connect);
	}

	private void whenTheConnectorIsStarted(Map<String, String> config, KafkaIntegrationTests.KafkaConnect connect) {
		connect.herder().putConnectorConfig(config.get("name"),
			config, false, (e, s) -> {
			});
	}

	private void thenFilesAreWrittenToGCS(Storage storage, String sinkTopic, List<Long> offsets) {
		waitForPassing(Duration.ofSeconds(10), () -> {
//			List<String> keys = gcs.listObjects(GCS_BUCKET, GCS_PREFIX).getObjectSummaries().stream()
//				.map(GCSObjectSummary::getKey).collect(toList());
			Page<Blob> blobs =  storage.list(
				GCS_BUCKET,
				Storage.BlobListOption.prefix(GCS_PREFIX)
			);
			List<String> keys = StreamSupport.stream(blobs.iterateAll().spliterator(), false)
				.map(Blob::getName).collect(toList());
			Set<Map.Entry<Integer, Long>> partAndOffset = keys.stream()
				.filter(key -> key.endsWith(".gz") && key.contains(sinkTopic))
				.map(key -> immutableEntry(Integer.parseInt(key.replaceAll(".*?-(\\d{5})-\\d{12}\\.gz", "$1")),
					Long.parseLong(key.replaceAll(".*?-\\d{5}-(\\d{12})\\.gz", "$1"))))
				.collect(toSet());

			Map<Integer, Long> startOffsets = transformValues(partAndOffset.stream()
					.collect(groupingBy(Map.Entry::getKey, minBy(Map.Entry.comparingByValue()))),
				(optEntry) -> optEntry.map(Map.Entry::getValue).orElse(0L));

			assertTrue(ofNullable(startOffsets.get(0)).orElse(-1L) >= offsets.get(0), startOffsets + "[0] !~ " + offsets);
			assertTrue(ofNullable(startOffsets.get(1)).orElse(-1L) >= offsets.get(1), startOffsets + "[1] !~ " + offsets);
		});

	}

	private Map<String, String> givenSourceConfig(String sourceTopic, String sinkTopic) throws IOException {
		return gcsConfig(ImmutableMap.<String, String>builder()
			.put("name", sourceTopic + "-gcs-source")
			.put("connector.class", GCSSourceConnector.class.getName())
			.put("tasks.max", "2")
			.put("topics", sinkTopic)
			.put("max.partition.count", "2")
			.put(GCSSourceTask.CONFIG_TARGET_TOPIC + "." + sinkTopic, sourceTopic))
			.build();
	}

	private ImmutableMap.Builder<String, String> gcsConfig(ImmutableMap.Builder<String, String> builder) {
		return builder
			.put("format", ByteLengthFormat.class.getName())
			.put("format.include.keys", "true")
			.put("key.converter", AlreadyBytesConverter.class.getName())

			.put("gcs.new.record.poll.interval", "200") // poll fast

			.put("gcs.bucket", GCS_BUCKET)
			.put("gcs.prefix", GCS_PREFIX)
			.put("gcs.endpoint", gcs.getEndpoint())
			.put("gcs.path_style", "true");  // necessary for FakeGCS
	}

//	private Map<String, String> givenSinkConfig(String sinkTopic) throws IOException {
//		File tempDir = Files.createTempDir();
//		// start sink connector
//		return gcsConfig(ImmutableMap.<String, String>builder()
//			.put("name", sinkTopic + "-gcs-sink")
//			.put("connector.class", GCSSinkConnector.class.getName())
//			.put("tasks.max", "2")
//			.put("topics", sinkTopic)
//			.put("local.buffer.dir", tempDir.getCanonicalPath()))
//			.build();
//	}

	private void givenRecords(String originalTopic, Producer<String, String> producer, int start) {
		// send odds to partition 1, and evens to 0
		Collection<Header> headers = new ArrayList<>();
		headers.add(new BasicHeader("hk", "hv"));
		IntStream.range(start, start + 4).forEach(i -> {
			try {
				producer.send(new ProducerRecord<>(originalTopic, i % 2, "key:" + i, "value:" + i)).get(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				throw Throwables.propagate(e);
			}
		});
	}


	private List<ConsumerRecord<String, String>> consumeTopic(final String topic, final int numberOfMessages) {
		Consumer<String, String> consumer = givenAConsumer();
		ImmutableList<TopicPartition> partitions = ImmutableList.of(
			new TopicPartition(topic, 0),
			new TopicPartition(topic, 1)
		);
		consumer.assign(partitions);
		consumer.seekToBeginning(partitions);

		List<ConsumerRecord<String, String>> results = new ArrayList<>();
		waitForPassing(Duration.ofSeconds(30), () -> {
			ConsumerRecords<String, String> records = consumer.poll(500L);
			StreamSupport.stream(records.spliterator(), false)
				.filter(r -> !"skip".equals(r.key()))
				.forEach(results::add);
			assertEquals(numberOfMessages, results.size(), "Not all of the expected messages exist on the topic");
		});
		return results;
	}

	private void thenTempFilesAreCleanedUp(Map<String, String> sinkConfig) {
		//noinspection ConstantConditions
		waitForPassing(Duration.ofSeconds(3), () ->
			assertEquals(ImmutableList.of(), ImmutableList.copyOf(new File(sinkConfig.get("local.buffer.dir")).list())));
	}


	private void thenMessagesAreRestored(String sourceTopic, Storage storage) {
		thenMessagesAreRestored(sourceTopic, 0, gcs.storageClient);
	}

	private void thenMoreMessagesAreRestored(String sourceTopic, Storage storage) {
		thenMessagesAreRestored(sourceTopic, 5, gcs.storageClient);
	}

	private void thenMessagesAreRestored(String sourceTopic, int start, Storage storage) {
		Consumer<String, String> consumer = givenAConsumer();
		ImmutableList<TopicPartition> partitions = ImmutableList.of(
			new TopicPartition(sourceTopic, 0),
			new TopicPartition(sourceTopic, 1)
		);
		consumer.assign(partitions);
		consumer.seekToBeginning(partitions);

		// HACK - add to this list and then assert we are done. will retry until done.
		List<List<String>> results = ImmutableList.of(
			new ArrayList<>(),
			new ArrayList<>()
		);
		waitForPassing(Duration.ofSeconds(30), () -> {
			ConsumerRecords<String, String> records = consumer.poll(500L);
			StreamSupport.stream(records.spliterator(), false)
				.filter(r -> !"skip".equals(r.key()))
				.forEach(r -> results.get(r.partition()).add(r.value()));
			assertEquals(2, distinctAfter(start, results.get(0)).count(), "got all the records for 0 " + results);
			assertEquals(2, distinctAfter(start, results.get(1)).count(), "got all the records for 1 " + results);
		});

		boolean startOdd = (start % 2 == 1);
		List<String> evens = results.get(0).stream().distinct().skip(start / 2).collect(toList());
		List<String> odds = results.get(1).stream().distinct().skip(start / 2).collect(toList());
		ImmutableList<ImmutableList<String>> expected = ImmutableList.of(
			ImmutableList.of("value:" + (0 + start), "value:" + (2 + start)),
			ImmutableList.of("value:" + (1 + start), "value:" + (3 + start))
		);
		ImmutableList<List<String>> actual = ImmutableList.of(
			startOdd ? odds : evens,
			startOdd ? evens : odds
		);
		if (!expected.equals(actual)) {
			// list the GCS chunks, for debugging
			Page<Blob> blobs =gcs.storageClient.list(
				GCS_BUCKET,
				Storage.BlobListOption.prefix(GCS_PREFIX)
			);
			String chunks = StreamSupport.stream(blobs.iterateAll().spliterator(), false).filter(
				s -> s.getName().endsWith(".json")
				)
				.flatMap(idx -> {
					try {
						return Stream.concat(Stream.of("\n===" + idx.getName() + "==="),
							((ChunksIndex) new ObjectMapper().readerFor(ChunksIndex.class).readValue(
								gcs.storageClient.get(GCS_BUCKET, idx.getName()).getContent()
							))
								.chunks.stream().map(c -> String.format("[%d..%d]",
									c.first_record_offset, c.first_record_offset + c.num_records)));
					} catch (IOException e) {
						return null;
					}
				}).collect(joining("\n"));

			// NOTE: results is asserted to assist in debugging. it may contain values that should be skipped
			assertEquals(expected, results, "records restored to same partitions, in-order. Chunks = " + chunks);
		}

		consumer.close();
	}

	private static void tryClose(AutoCloseable doClose) {
		try {
			doClose.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void whenTheSinkIsStopped(String name) {
		connect.herder().putConnectorConfig(name, null, true,
			(e, c) -> {
				if (e != null) Throwables.propagate(e);
			});
	}

	// this seems to take at least 20 seconds to get a segment deleted :(
	private Map.Entry<String, List<Long>> slow_givenATopicWithNonZeroStartOffsets() throws InterruptedException, ExecutionException, TimeoutException {
		Map<String, String> props = new HashMap<>();
		props.put("segment.bytes", "100");
		props.put("segment.ms", "500");
		props.put("retention.ms", "500");
		String topic = kafka.createUniqueTopic("non-zero-", 2, props);
		Producer<String, String> producer = givenKafkaProducer();

		// produce a bunch of records
		for (int i = 0; i < 200; i++) {
			producer.send(new ProducerRecord<>(topic, i % 2, "skip", "ignore")).get(5, TimeUnit.SECONDS);
		}
		Consumer<String, String> consumer = givenAConsumer();
		consumer.assign(ImmutableList.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
		ImmutableList<Long> offsets = waitForPassing(Duration.ofMinutes(10), () -> {
			consumer.seekToBeginning(ImmutableList.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));

			long zero = consumer.position(new TopicPartition(topic, 0));
			long one = consumer.position(new TopicPartition(topic, 1));
			assertNotEquals(0L, zero, "partition 0: 0 has rolled out");
			assertNotEquals(0L, one, "partition 1: 0 has rolled out");

			return ImmutableList.of(zero, one);
		});
		consumer.close();
		// revert the retention to something reasonable
		props.put("retention.ms", "300000");
		kafka.updateTopic(topic, props);
		return immutableEntry(topic, offsets);
	}

	private Map.Entry<String, List<Long>> givenATopicWithZeroStartOffsets() {
		Map<String, String> props = new HashMap<>();
		props.put("segment.ms", "500");
		props.put("retention.ms", "300000");
		String topic = kafka.createUniqueTopic("zero-", 2, props);

		Consumer<String, String> consumer = givenAConsumer();
		consumer.assign(ImmutableList.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
		ImmutableList<Long> offsets = waitForPassing(Duration.ofSeconds(10), () -> {
			consumer.seekToBeginning(ImmutableList.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));

			long zero = consumer.position(new TopicPartition(topic, 0));
			long one = consumer.position(new TopicPartition(topic, 1));
			assertEquals(0L, zero, "partition 0: 0 has rolled out");
			assertEquals(0L, one, "partition 1: 0 has rolled out");

			return ImmutableList.of(zero, one);
		});
		consumer.close();

		return immutableEntry(topic, offsets);
	}

	private Stream<String> distinctAfter(int start, List<String> list) {
		return list.stream().distinct().skip(start / 2);
	}

	private Producer<String, String> givenKafkaProducer() {
		return new KafkaProducer<>(ImmutableMap.<String, Object>builder()
			.put("bootstrap.servers", "localhost:" + kafka.getLocalPort())
			.put("key.serializer", StringSerializer.class.getName())
			.put("value.serializer", StringSerializer.class.getName())
			.put("retries", "5")
			.put("acks", "all")
			.build());
	}


	public static class TestRecord {
		String key;
		String value;
		Headers headers;

		public TestRecord(final String key, final String value, final Headers headers) {
			this.key = key;
			this.value = value;
			this.headers = headers;
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			final TestRecord that = (TestRecord) o;
			return Objects.equals(key, that.key) && Objects.equals(value, that.value) && Objects.equals(headers, that.headers);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, value, headers);
		}

		@Override
		public String toString() {
			return "TestRecord{" +
				"key='" + key + '\'' +
				", value='" + value + '\'' +
				", headers='" + headers + '\'' +
				'}';
		}
	}

}
