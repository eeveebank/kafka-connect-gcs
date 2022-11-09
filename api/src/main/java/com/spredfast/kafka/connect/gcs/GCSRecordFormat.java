package com.spredfast.kafka.connect.gcs;

/**
 * Pairing of reader and writer for records in S3. A new reader/writer will be constructed for each
 * file to be read/written.
 */
public interface GCSRecordFormat {

	/**
	 * Returns a function that takes a topic and raw bytes, and returns the bytes to write to S3.
	 * Bytes for each record will be written consecutively without any additional delimiter, so the
	 * reader must be able to read a concatenation of such byte sequences.
	 */
	GCSRecordsWriter newWriter();

	/**
	 * @return a reader that can reverse {@link #newWriter()}
	 */
	GCSRecordsReader newReader();

	/**
	 * Convenience method if you have your own S3 data you want to read out.
	 */
	static GCSRecordFormat readOnly(GCSRecordsReader reader) {
		return from(reader, r -> { throw new UnsupportedOperationException("Format is read-only."); });
	}

	static GCSRecordFormat from(GCSRecordsReader reader, GCSRecordsWriter writer) {
		return new GCSRecordFormat() {
			@Override
			public GCSRecordsWriter newWriter() {
				return writer;
			}

			@Override
			public GCSRecordsReader newReader() {
				return reader;
			}
		};
	}
}

