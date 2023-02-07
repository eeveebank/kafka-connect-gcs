package com.spredfast.kafka.connect.gcs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;

import static com.spredfast.kafka.connect.gcs.Constants.LENGTH_FIELD_SIZE;
import static com.spredfast.kafka.connect.gcs.FormatTests.assertBytesAreEqual;
import static org.junit.Assert.assertEquals;

public class ByteLengthFormatTest {

	//	INFO 2023-02-07T09:35:51.342823112Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 88, -27]
	//	INFO 2023-02-07T09:35:51.775160951Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 4, 44]
	//	INFO 2023-02-07T09:35:54.339323233Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 96, -99]
	//	INFO 2023-02-07T09:35:55.029105658Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:35:55.998989667Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 5, 126]
	//	INFO 2023-02-07T09:35:56.537830729Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 5, -14]
	//	INFO 2023-02-07T09:35:56.611707473Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 5, 95]
	//	INFO 2023-02-07T09:35:58.113574669Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:35:59.700810289Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 5, -5]
	//	INFO 2023-02-07T09:35:59.819363771Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:35:59.892236309Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:36:00.476222119Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:36:00.935755001Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 107]
	//	INFO 2023-02-07T09:37:40.979941685Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 96, -99]
	//	INFO 2023-02-07T09:37:44.682687725Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:37:44.922900745Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:37:44.948250846Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 0, 36]
	//	INFO 2023-02-07T09:37:45.102478943Z [resource.labels.containerName: kafka-connect-gcs-restore-v47] info readLen bytesAsString [0, 0, 5, 95]

	@Test
	public void tempDebugBytes() throws IOException {
		BufferedInputStream data;
		InputStream input = new ByteArrayInputStream(new byte[] { 0, 0, 0, 36 });
		ByteBuffer lenBuffer = ByteBuffer.allocate(LENGTH_FIELD_SIZE);
		lenBuffer.rewind();
		int code = input.read(lenBuffer.array(), 0, 4);
		assertEquals(code, 4);
	}

	@Test
	public void defaults() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withNullKeys(givenFormatWithConfig(ImmutableMap.of()),
			ImmutableList.of(
				FormatTests.Record.valueOnly("abcd"),
				FormatTests.Record.valueOnly("567\tav"),
				FormatTests.Record.valueOnly("238473210984712309\n84710923847231098472390847324098543298652938475\n49837")
			)
		);
	}

	@Test
	public void withKeys() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withKeys(givenFormatWithConfig(ImmutableMap.of("include.keys", "true")),
			ImmutableList.of(
				FormatTests.Record.keysAndValueOnly("k1", "abcd"),
				FormatTests.Record.keysAndValueOnly("k2", "567\tav"),
				FormatTests.Record.keysAndValueOnly("k3", "238473210984712309\n84710923847231098472390847324098543298652938475\n49837")
			),
			0);
	}

	@Test
	public void withKeysAndHeaders() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withKeysAndHeaders(givenFormatWithConfig(ImmutableMap.of("include.keys", "true")),
			ImmutableList.of(
				new FormatTests.Record("k1", "abcd", new RecordHeaders()),
				new FormatTests.Record("k2", "567\tav", new RecordHeaders(new RecordHeader[]{
					new RecordHeader("h1", "".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("h2", (byte[]) null),
					new RecordHeader("h3", "foo".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("h4", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
				})),
				new FormatTests.Record("k3", "\u0006GET\u0002H396a14e5-5f45-455a-b840-afdca1625748\u0014/auth/user\u0000", new RecordHeaders(new RecordHeader[]{
					new RecordHeader("h1", "foo".getBytes(StandardCharsets.UTF_8)),
				})),
				new FormatTests.Record("089dc5b2-b8a7-4b0a-be66-f20cfe79cc5b", "238473210984712309\n84710923847231098472390847324098543298652938475\n49837", new RecordHeaders(new RecordHeader[]{
					new RecordHeader("produce.timestamp.epoch_ms.string", "1672420145594".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("spring_json_header_types", "{\"produce.timestamp.epoch_ms.string\":\"java.lang.String\",\"createdTimestamp\":\"java.lang.Long\",\"contentType\":\"java.lang.String\",\"stateStoreId\":\"java.lang.String\"}".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("uber-trace-id", "9e9f00c502f8c71b2c0dfe6bfe1063b7:f8a6085bfe88f882:0:1".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("createdTimestamp", "1672420145594".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("traceparent", "00-9e9f00c502f8c71b2c0dfe6bfe1063b7-f8a6085bfe88f882-01".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("contentType", "application/vnd.opsuseraction.v2+avro".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("stateStoreId", "e7efc4f3-c250-485d-b6ea-61d307320000".getBytes(StandardCharsets.UTF_8)),
				}))
			),
			0);
	}


	@Test
	public void outputWithKeys() {
		ByteLengthFormat format = givenFormatWithConfig(ImmutableMap.of("include.keys", "true"));

		byte[] key = "abc".getBytes(StandardCharsets.UTF_8);
		byte[] value = "defghi".getBytes(StandardCharsets.UTF_8);

		int numberOfByteForLengthMarker = 4;

		byte[] expected = new byte[numberOfByteForLengthMarker + key.length + numberOfByteForLengthMarker + value.length];
		ByteBuffer buffer = ByteBuffer.wrap(expected);

		buffer.putInt(key.length);
		buffer.put(key);
		buffer.putInt(value.length);
		buffer.put(value);

		assertBytesAreEqual(expected, format.newWriter().writeBatch(Stream.of(new ProducerRecord<>("topic", key, value))).findFirst().get());
	}

	@Test
	public void outputWithKeysAndHeaders() {
		ByteLengthFormat format = givenFormatWithConfig(ImmutableMap.of("include.keys", "true"));

		byte[] key = "abc".getBytes(StandardCharsets.UTF_8);
		byte[] value = "defghi".getBytes(StandardCharsets.UTF_8);
		Headers headers = new RecordHeaders(new RecordHeader[]{new RecordHeader("h1", "foo".getBytes(StandardCharsets.UTF_8))});
		byte[] serialisedHeaders = "[{\"key\":\"h1\",\"value\":[102,111,111]}]".getBytes(StandardCharsets.UTF_8);

		int numberOfByteForLengthMarker = 4;
		int numberOfByteForHeaderMarker = 1;

		byte[] expected = new byte[numberOfByteForLengthMarker + key.length +
			numberOfByteForLengthMarker + value.length +
			numberOfByteForLengthMarker + numberOfByteForHeaderMarker + serialisedHeaders.length
			];
		ByteBuffer buffer = ByteBuffer.wrap(expected);

		buffer.putInt(key.length);
		buffer.put(key);
		buffer.putInt(value.length);
		buffer.put(value);
		buffer.put((byte) -10);
		buffer.putInt(serialisedHeaders.length);
		buffer.put(serialisedHeaders);

		assertBytesAreEqual(expected, format.newWriter().writeBatch(Stream.of(new ProducerRecord<>("topic", null, key, value, headers))).findFirst().get());
	}

	private ByteLengthFormat givenFormatWithConfig(ImmutableMap<String, Object> configs) {
		ByteLengthFormat format = new ByteLengthFormat();
		format.configure(configs);
		return format;
	}

}
