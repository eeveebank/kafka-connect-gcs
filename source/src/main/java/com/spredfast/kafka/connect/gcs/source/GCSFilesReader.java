package com.spredfast.kafka.connect.gcs.source;

import com.amazonaws.AmazonClientException;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Blob;
//import com.amazonaws.services.gcs.model.GetObjectRequest;
//import com.amazonaws.services.gcs.model.ListObjectsRequest;
//import com.amazonaws.services.gcs.model.ObjectListing;
//import com.amazonaws.services.gcs.model.GCSObject;
//import com.amazonaws.services.gcs.model.GCSObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.spredfast.kafka.connect.gcs.LazyString;
import com.spredfast.kafka.connect.gcs.GCSRecordsReader;
import com.spredfast.kafka.connect.gcs.json.ChunkDescriptor;
import com.spredfast.kafka.connect.gcs.json.ChunksIndex;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static java.util.stream.Collectors.toList;

/**
 * Helpers for reading records out of GCS. Not thread safe.
 * Records should be in order since GCS lists files in lexicographic order.
 * It is strongly recommended that you use a unique key prefix per topic as
 * there is no option to restrict this reader by topic.
 * <p>
 * NOTE: hasNext() on the returned iterators may throw AmazonClientException if there
 * was a problem communicating with GCS or reading an object. Your code should
 * catch AmazonClientException and implement back-off and retry as desired.
 * <p>
 * Any other exception should be considered a permanent failure.
 */
public class GCSFilesReader implements Iterable<GCSSourceRecord> {

	private static final Logger log = LoggerFactory.getLogger(GCSFilesReader.class);

	public static final Pattern DEFAULT_PATTERN = Pattern.compile(
		"(\\/|^)"                        // match the / or the start of the key so we shouldn't have to worry about prefix
			+ "(?<topic>[^/]+?)-"            // assuming no / in topic names
			+ "(?<partition>\\d{5})-"
			+ "(?<offset>\\d{12})\\.gz$"
	);

	private final Storage storage;

	private final Supplier<GCSRecordsReader> makeReader;

	private final Map<GCSPartition, GCSOffset> offsets;

	private final ObjectReader indexParser = new ObjectMapper().reader(ChunksIndex.class);

	private final GCSSourceConfig config;

	public GCSFilesReader(GCSSourceConfig config, Storage storage, Map<GCSPartition, GCSOffset> offsets, Supplier<GCSRecordsReader> recordReader) {
		this.config = config;
		this.offsets = Optional.ofNullable(offsets).orElseGet(HashMap::new);
		this.storage = storage;
		this.makeReader = recordReader;
	}

	public Iterator<GCSSourceRecord> iterator() {
		return readAll();
	}

	public interface PartitionFilter {
		// convenience for simple filters. Only the 2 argument version will ever be called.
		boolean matches(int partition);

		default boolean matches(String topic, int partition) {
			return matches(partition);
		}

		static PartitionFilter from(BiPredicate<String, Integer> filter) {
			return new PartitionFilter() {
				@Override
				public boolean matches(int partition) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean matches(String topic, int partition) {
					return filter.test(topic, partition);
				}
			};
		}

		PartitionFilter MATCH_ALL = p -> true;
	}

	private static final Pattern DATA_SUFFIX = Pattern.compile("\\.gz$");

	private int partition(String key) {
		final Matcher matcher = config.keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		return Integer.parseInt(matcher.group("partition"));
	}

	private String topic(String key) {
		final Matcher matcher = config.keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		return matcher.group("topic");
	}

	public Iterator<GCSSourceRecord> readAll() {
		return new Iterator<GCSSourceRecord>() {
			String currentKey;

			ObjectListing objectListing;
			Iterator<GCSObjectSummary> nextFile = Collections.emptyIterator();
			Iterator<ConsumerRecord<byte[], byte[]>> iterator = Collections.emptyIterator();

			private void nextObject() {
				while (!nextFile.hasNext() && hasMoreObjects()) {

					// partitions will be read completely for each prefix (e.g., a day) in order.
					// i.e., all of partition 0 will be read before partition 1. Seems like that will make perf wonky if
					// there is an active, multi-partition consumer on the other end.
					// to mitigate that, have as many tasks as partitions.
					if (objectListing == null) {
						// https://github.com/googleapis/java-storage/blob/583bf73f5d58aa5d79fbaa12b24407c558235eed/samples/snippets/src/main/java/com/example/storage/object/ListObjectsWithPrefix.java
						Page<Blob> blobs = storage.list(
							config.bucket,
							Storage.BlobListOption.prefix(config.keyPrefix),
							Storage.BlobListOption.pageToken(config.startMarker)
						);
//						objectListing = storage.listObjects(new ListObjectsRequest(
//							config.bucket,
//							config.keyPrefix,
//							config.startMarker,
//							null,
//							// we have to filter out chunk indexes on this end, so
//							// whatever the requested page size is, we'll need twice that
//							config.pageSize * 2
//						));
						log.debug("gcs ls {}/{} after:{} = {}", config.bucket, config.keyPrefix, config.startMarker,
							LazyString.of(() -> objectListing.getObjectSummaries().stream().map(GCSObjectSummary::getName).collect(toList())));
					} else {
						String marker = objectListing.getNextMarker();
						objectListing = storage.listNextBatchOfObjects(objectListing);
						log.debug("aws ls {}/{} after:{} = {}", config.bucket, config.keyPrefix, marker,
							LazyString.of(() -> objectListing.getObjectSummaries().stream().map(GCSObjectSummary::getName).collect(toList())));
					}

					List<GCSObjectSummary> chunks = new ArrayList<>(objectListing.getObjectSummaries().size() / 2);
					for (GCSObjectSummary chunk : objectListing.getObjectSummaries()) {
						if (DATA_SUFFIX.matcher(chunk.getName()).find() && parseKeyUnchecked(chunk.getName(),
							(t, p, o) -> config.partitionFilter.matches(t, p))) {
							GCSOffset offset = offset(chunk);
							if (offset != null) {
								// if our offset for this partition is beyond this chunk, ignore it
								// this relies on filename lexicographic order being correct
								if (offset.getGCSkey().compareTo(chunk.getName()) > 0) {
									log.debug("Skipping {} because < current offset of {}", chunk.getName(), offset);
									continue;
								}
							}
							chunks.add(chunk);
						}
					}
					log.debug("Next Chunks: {}", LazyString.of(() -> chunks.stream().map(GCSObjectSummary::getName).collect(toList())));
					nextFile = chunks.iterator();
				}
				if (!nextFile.hasNext()) {
					iterator = Collections.emptyIterator();
					return;
				}
				try {
					GCSObjectSummary file = nextFile.next();

					currentKey = file.getName();
					GCSOffset offset = offset(file);
					if (offset != null && offset.getGCSkey().equals(currentKey)) {
						resumeFromOffset(offset);
					} else {
						log.debug("Now reading from {}", currentKey);
						GCSRecordsReader reader = makeReader.get();
						InputStream content = getContent(storage.getObject(config.bucket, currentKey));
						iterator = parseKey(currentKey, (topic, partition, startOffset) -> {
							reader.init(topic, partition, content, startOffset);
							return reader.readAll(topic, partition, content, startOffset);
						});
					}
				} catch (IOException e) {
					throw new AmazonClientException(e);
				}
			}

			private InputStream getContent(GCSObject object) throws IOException {
				return config.inputFilter.filter(object.getObjectContent());
			}

			private GCSOffset offset(GCSObjectSummary chunk) {
				return offsets.get(GCSPartition.from(config.bucket, config.keyPrefix, topic(chunk.getName()), partition(chunk.getName())));
			}

			/**
			 * If we have a non-null offset to resume from, then our marker is the current file, not the next file,
			 * so we need to load the marker and find the offset to start from.
			 */
			private void resumeFromOffset(GCSOffset offset) throws IOException {
				log.debug("resumeFromOffset {}", offset);
				GCSRecordsReader reader = makeReader.get();

				ChunksIndex index = getChunksIndex(offset.getGCSkey());
				ChunkDescriptor chunkDescriptor = index.chunkContaining(offset.getOffset() + 1)
					.orElse(null);

				if (chunkDescriptor == null) {
					log.warn("Missing chunk descriptor for requested offset {} (max:{}). Moving on to next file.",
						offset, index.lastOffset());
					// it's possible we were at the end of this file,
					// so move on to the next one
					nextObject();
					return;
				}

				// if we got here, it is a real object and contains
				// the offset we want to start at

				// if need the start of the file for the read, let it read it
				if (reader.isInitRequired() && chunkDescriptor.byte_offset > 0) {
//					BlobId blobId = BlobId.of(config.bucket, offset.getGCSkey());
//					Blob blob2 = storage.get(blobId);
					Blob object = storage.get(BlobId.of(config.bucket, offset.getGCSkey()));
					parseKey(object.getName(), (topic, partition, startOffset) -> {
						reader.init(topic, partition, getContent(object), startOffset);
						return null;
					});
//					try (GCSObject object = storage.getObject(new GetObjectRequest(config.bucket, offset.getGCSkey()))) {
//						parseKey(object.getName(), (topic, partition, startOffset) -> {
//							reader.init(topic, partition, getContent(object), startOffset);
//							return null;
//						});
//					}
				}

				GetObjectRequest request = new GetObjectRequest(config.bucket, offset.getGCSkey());
				request.setRange(chunkDescriptor.byte_offset, index.totalSize());

				Blob object = storage.get(request);

				currentKey = object.getName();
				log.debug("Resume {}: Now reading from {}, reading {}-{}", offset, currentKey, chunkDescriptor.byte_offset, index.totalSize());

				iterator = parseKey(object.getName(), (topic, partition, startOffset) ->
					reader.readAll(topic, partition, getContent(object), chunkDescriptor.first_record_offset));

				// skip records before the given offset
				long recordSkipCount = offset.getOffset() - chunkDescriptor.first_record_offset + 1;
				for (int i = 0; i < recordSkipCount; i++) {
					iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				while (!iterator.hasNext() && hasMoreObjects()) {
					nextObject();
				}
				return iterator.hasNext();
			}

			boolean hasMoreObjects() {
				return objectListing == null || objectListing.isTruncated() || nextFile.hasNext();
			}

			@Override
			public GCSSourceRecord next() {
				ConsumerRecord<byte[], byte[]> record = iterator.next();
				return new GCSSourceRecord(
					GCSPartition.from(config.bucket, config.keyPrefix, record.topic(), record.partition()),
					GCSOffset.from(currentKey, record.offset()),
					record.topic(),
					record.partition(),
					record.key(),
					record.value(),
					toConnectHeaders(record.headers())
				);
			}

			private Headers toConnectHeaders(final org.apache.kafka.common.header.Headers commonHeaders) {
				if (commonHeaders == null) {
					return null;
				}

				ConnectHeaders connectHeaders = new ConnectHeaders();
				for (Header header : commonHeaders) {
					String value = header.value() == null ? null : new String(header.value());
					Schema schema = value == null ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
					connectHeaders.add(header.key(), new SchemaAndValue(schema, value));
				}
				return connectHeaders;
			}


			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private <T> T parseKeyUnchecked(String key, QuietKeyConsumer<T> consumer) {
		try {
			return parseKey(key, consumer::consume);
		} catch (IOException never) {
			throw new RuntimeException(never);
		}
	}

	private <T> T parseKey(String key, KeyConsumer<T> consumer) throws IOException {
		final Matcher matcher = config.keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		final String topic = matcher.group("topic");
		final int partition = Integer.parseInt(matcher.group("partition"));
		final long startOffset = Long.parseLong(matcher.group("offset"));

		return consumer.consume(topic, partition, startOffset);
	}


	private interface QuietKeyConsumer<T> {
		T consume(String topic, int partition, long startOffset);
	}

	private interface KeyConsumer<T> {
		T consume(String topic, int partition, long startOffset) throws IOException;
	}

	private ChunksIndex getChunksIndex(String key) throws IOException {
		return indexParser.readValue(new InputStreamReader(storage.getObject(config.bucket, DATA_SUFFIX.matcher(key)
			.replaceAll(".index.json")).getObjectContent()));
	}

	/**
	 * Filtering applied to the GCSInputStream. Will almost always start
	 * with GUNZIP, but could also include things like decryption.
	 */
	public interface InputFilter {
		InputStream filter(InputStream inputStream) throws IOException;

		InputFilter GUNZIP = GZIPInputStream::new;
	}

}
