package com.spredfast.kafka.connect.gcs;

import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 */
public interface GCSRecordsWriter {

	/**
	 * Opportunity to write any header bytes desired.
	 */
	default byte[] init(String topic, int partition, long startOffset) {
		return new byte[0];
	}

	/**
	 * Called multiple times to encode a set of records. Should return one byte array per record.
	 */
	Stream<byte[]> writeBatch(Stream<ProducerRecord<byte[], byte[]>> records);

	/**
	 * Hook for writing any trailer bytes to the S3 file.
	 */
	default byte[] finish(String topic, int partition) {
		return new byte[0];
	}

	static GCSRecordsWriter forRecordWriter(Function<ProducerRecord<byte[], byte[]>, byte[]> writeRecord) {
		return records -> records.map(writeRecord);
	}
}
