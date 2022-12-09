package com.spredfast.kafka.connect.gcs;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.spredfast.kafka.connect.gcs.source.GCSFilesReader;
import com.spredfast.kafka.connect.gcs.source.GCSOffset;
import com.spredfast.kafka.connect.gcs.source.GCSPartition;
import com.spredfast.kafka.connect.gcs.source.GCSSourceConfig;
import com.spredfast.kafka.connect.gcs.source.GCSSourceRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers GCS and reading raw byte records. Closer to an integration test.
 */
class GCSFilesReaderTest {
	private final FakeGCS gcs = new FakeGCS();
	private String bucketName = "bucket";
	private static final Random RANDOM = new Random();

	private static final BiFunction<String, String, Headers> headersFunction = (k, v) -> new RecordHeaders(new RecordHeader[]{new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))});
	static Storage storageClient;


	@BeforeEach
	void setUp() throws Exception {
		bucketName = String.format("test-bucket-%d", RANDOM.nextLong());
		storageClient = gcs.startAndReturnClient(bucketName);
	}

	@AfterEach
	void tearDown() {
		gcs.close();
	}

	@Test
	void testReadingBytesFromGCS() throws IOException {
		final Storage client = storageClient;
		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenSomeDataWithKeys(client, dir);

		List<String> results = whenTheRecordsAreRead(client, true, 3);

		thenTheyAreInOrder(results);
	}

	@Test
	void testReadingBytesFromGCS_multiPartition() throws IOException {
		// scenario: multiple partition files at the end of a listing, page size >  # of files
		// do we read all of them?
		final Storage client = storageClient;
		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenASingleDayWithManyPartitions(client, dir);

		List<String> results = whenTheRecordsAreRead(client, true, 10);

		thenTheyAreInOrder(results);
	}

	@Test
	void testReadingBytesFromGCS_withOffsets() throws IOException {
		final Storage client = storageClient;
		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenSomeDataWithKeys(client, dir);

		List<String> results = whenTheRecordsAreRead(
			givenAReaderWithOffsets(
				client,
				"prefix/2015-12-31/topic-00003-000000000001.gz",
				5L,
				"00003"
			)
		);

		assertEquals(Arrays.asList(
			"willbe=skipped5[skipped header key5:skipped header value5]",
			"willbe=skipped6[skipped header key6:skipped header value6]",
			"willbe=skipped7[skipped header key7:skipped header value7]",
			"willbe=skipped8[skipped header key8:skipped header value8]",
			"willbe=skipped9[skipped header key9:skipped header value9]"
		), results);
	}


	@Test
	void testReadingBytesFromGCS_withOffsetsAtEndOfFile() throws IOException {
		final Storage client = storageClient;
		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenSomeDataWithKeys(client, dir);

		// this file will be skipped
		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/2015-12-30/topic-00003-000000000000.gz", 1L, "00003"));

		assertEquals(
			IntStream.range(1, 10)
				.mapToObj(i -> String.format("willbe=skipped%d[skipped header key%d:skipped header value%d]", i, i, i))
				.collect(toList()),
			results);
	}

	GCSFilesReader givenAReaderWithOffsets(Storage client, String marker, long nextOffset, final String partition) {
		Map<GCSPartition, GCSOffset> offsets = new HashMap<>();
		int partInt = Integer.valueOf(partition, 10);
		offsets.put(
			GCSPartition.from(bucketName, "prefix", "topic", partInt),
			GCSOffset.from(marker, nextOffset - 1 /* an GCS offset is the last record processed, so go back 1 to consume next */)
		);
		return new GCSFilesReader(
			new GCSSourceConfig(
				bucketName,
				"prefix",
				1,
				null,
				GCSFilesReader.DEFAULT_PATTERN,
				GCSFilesReader.InputFilter.GUNZIP,
				p -> partInt == p
			),
			client,
			offsets,
			() -> new BytesRecordReader(true)
		);
	}

	@Test
	void testReadingBytesFromGCS_withoutKeys() throws IOException {
		final Storage client = storageClient;
		final Path dir = Files.createTempDirectory("gcsFilesReaderTest");
		givenSomeDataWithoutKeys(client, dir);

		List<String> results = whenTheRecordsAreRead(client, false);

		thenTheyAreInOrderWithoutKeys(results);
	}

	void thenTheyAreInOrderWithoutKeys(List<String> results) {
		List<String> expected = Arrays.asList(
			"value0-0[header key 0-0:header value 0-0]",
			"value1-0[header key 1-0:header value 1-0]",
			"value1-1[header key 1-1:header value 1-1]"
		);
		assertEquals(expected, results);
	}

	private void thenTheyAreInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
			"key0-0=value0-0[header key 0-0:header value 0-0]",
			"key1-0=value1-0[header key 1-0:header value 1-0]",
			"key1-1=value1-1[header key 1-1:header value 1-1]"
		);
		assertEquals(expected, results);
	}

	private List<String> whenTheRecordsAreRead(Storage client, boolean fileIncludesKeys) {
		return whenTheRecordsAreRead(client, fileIncludesKeys, 1);
	}

	private List<String> whenTheRecordsAreRead(Storage storage, boolean fileIncludesKeys, int pageSize) {
		GCSFilesReader reader = new GCSFilesReader(
			new GCSSourceConfig(
				bucketName, "prefix", pageSize, "prefix/2016-01-01", GCSFilesReader.DEFAULT_PATTERN, GCSFilesReader.InputFilter.GUNZIP, null
			),
			storage,
			null,
			() -> new BytesRecordReader(fileIncludesKeys)
		);
		return whenTheRecordsAreRead(reader);
	}

	private List<String> whenTheRecordsAreRead(GCSFilesReader reader) {
		List<String> results = new ArrayList<>();
		for (GCSSourceRecord record : reader) {
			String key = (record.key() == null ? "" : new String(record.key()) + "=");
			String value = new String(record.value());
			String headers = StreamSupport.stream(record.headers().spliterator(), false)
				.map(h -> h.key() + ":" + (String) h.value())
				.collect(Collectors.joining(","));

			results.add(key + value + "[" + headers + "]");
		}
		return results;
	}

	private void givenASingleDayWithManyPartitions(Storage client, Path dir) throws IOException {
		givenASingleDayWithManyPartitions(client, dir, true);
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

	private void givenSomeDataWithKeys(Storage client, Path dir) throws IOException {
		givenSomeData(client, dir, true);
	}

	private void givenSomeDataWithoutKeys(Storage client, Path dir) throws IOException {
		givenSomeData(client, dir, false);
	}

//	private void givenSomeData(Storage client, Path dir, boolean includeKeys, Converter valueConverter) throws IOException {
//		givenSomeData(client, dir, includeKeys);
//	}

	private void givenSomeData(Storage client, Path dir, boolean includeKeys) throws IOException {
		new File(dir.toFile(), "prefix/2015-12-30").mkdirs();
		new File(dir.toFile(), "prefix/2015-12-31").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-01").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-02").mkdirs();
		try (BlockGZIPFileWriter writer0 = new BlockGZIPFileWriter("topic-00003", dir.toString() + "/prefix/2015-12-31", 1, 512);
			 BlockGZIPFileWriter writer1 = new BlockGZIPFileWriter("topic-00000", dir.toString() + "/prefix/2016-01-01", 0, 512);
			 BlockGZIPFileWriter writer2 = new BlockGZIPFileWriter("topic-00001", dir.toString() + "/prefix/2016-01-02", 0, 512);
			 BlockGZIPFileWriter preWriter1 = new BlockGZIPFileWriter("topic-00003", dir.toString() + "/prefix/2015-12-30", 0, 512);
		) {
			write(preWriter1, "willbe".getBytes(), "skipped0".getBytes(), headersFunction.apply("skipped header key", "skipped header value"), includeKeys);

			for (int i = 1; i < 10; i++) {
				write(writer0, "willbe".getBytes(), ("skipped" + i).getBytes(), headersFunction.apply("skipped header key" + i, "skipped header value" + i), includeKeys);
			}

			write(writer1, "key0-0".getBytes(), "value0-0".getBytes(), headersFunction.apply("header key 0-0", "header value 0-0"), includeKeys);

			write(writer2, "key1-0".getBytes(), "value1-0".getBytes(), headersFunction.apply("header key 1-0", "header value 1-0"), includeKeys);
			write(writer2, "key1-1".getBytes(), "value1-1".getBytes(), headersFunction.apply("header key 1-1", "header value 1-1"), includeKeys);
		}

		uploadToGCS(client, dir);
	}

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
			BlobId blobId = BlobId.of(bucketName, relative.toString());
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
			try {
				storage.create(blobInfo, Files.readAllBytes(f), precondition);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private void write(BlockGZIPFileWriter writer, byte[] key, byte[] value, Headers headers, boolean includeKeys) throws IOException {
		writer.write(new ByteLengthFormat(includeKeys).newWriter().writeBatch(Stream.of(
			new ProducerRecord<>("", 0, key, value, headers)
		)).collect(toList()), 1);
	}

}
