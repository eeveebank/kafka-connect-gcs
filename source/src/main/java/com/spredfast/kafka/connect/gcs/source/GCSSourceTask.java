package com.spredfast.kafka.connect.gcs.source;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.spredfast.kafka.connect.gcs.AlreadyBytesConverter;
import com.spredfast.kafka.connect.gcs.Configure;
import com.spredfast.kafka.connect.gcs.Constants;
import com.spredfast.kafka.connect.gcs.GCS;
import com.spredfast.kafka.connect.gcs.GCSRecordFormat;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.*;

public class GCSSourceTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger(GCSSourceTask.class);

	/**
	 * @see #remapTopic(String)
	 */
	public static final String CONFIG_TARGET_TOPIC = "targetTopic";
	public final AtomicBoolean stopped = new AtomicBoolean(); // public for testing

	private Map<String, String> taskConfig;
	private Iterator<GCSSourceRecord> reader;
	private int maxPoll;
	private final Map<String, String> topicMapping = new HashMap<>();
	private GCSRecordFormat format;
	private Optional<Converter> keyConverter;
	private Converter valueConverter;
	private long gcsPollInterval = 10_000L;
	private long errorBackoff = 1000L;
	public Map<GCSPartition, GCSOffset> offsets; // public for testing
	public GCSSourceConfig gcsSourceConfig; // public for testing
	public Storage gcsClient;  // public for testing

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> taskConfig) {
		this.taskConfig = taskConfig;
		format = Configure.createFormat(taskConfig);

		keyConverter = Optional.ofNullable(Configure.buildConverter(taskConfig, "key.converter", true, null));
		valueConverter = Configure.buildConverter(taskConfig, "value.converter", false, AlreadyBytesConverter.class);

		if (gcsClient == null) { // if not testing
			gcsClient = GCS.gcsclient(taskConfig);
		}

		readFromStoredOffsets();
	}

	private void readFromStoredOffsets() {
		try {
			tryReadFromStoredOffsets();
		} catch (Exception e) {
			throw new ConnectException("Couldn't start task " + taskConfig, e);
		}
	}

	private void tryReadFromStoredOffsets() {
		String bucket = configGet("gcs.bucket").orElseThrow(() -> new ConnectException("No bucket configured!"));
		String prefix = configGet("gcs.prefix").orElse("");

		Set<Integer> partitionNumbers = Arrays.stream(configGet("partitions").orElseThrow(
				() -> new IllegalStateException("no assigned partitions!?")
			).split(","))
			.map(Integer::parseInt)
			.collect(toSet());

		Set<String> topics = configGet("topics")
			.map(Object::toString)
			.map(s -> Arrays.stream(s.split(",")).collect(toSet()))
			.orElseGet(HashSet::new);

		Set<String> topicsToIgnore = configGet("topics.ignore")
			.map(Object::toString)
			.map(s -> Arrays.stream(s.split(",")).collect(toSet()))
			.orElseGet(HashSet::new);

		Boolean splitTopicsAcrossTasks = Boolean.parseBoolean(configGet("tasks.splitTopics").orElse("false"));

		List<GCSPartition> partitions = partitionNumbers
			.stream()
			.flatMap(partition -> topics
				.stream()
				.filter(topic -> (topicsToIgnore.isEmpty() || !topicsToIgnore.contains(topic))
					&& (!splitTopicsAcrossTasks || checkIfHashedTopicBelongsToTask(topic+"-"+partition))
				)
				.map(topic ->
					GCSPartition.from(bucket, prefix, topic, partition)
				)
			)
			.collect(toList());

		// need to maintain internal offset state forever. task will be committed and stopped if
		// our partitions change, so internal state should always be the most accurate
		if (offsets == null) {
			offsets = context.offsetStorageReader()
				.offsets(partitions.stream().map(GCSPartition::asMap).collect(toList()))
				.entrySet().stream().filter(e -> e.getValue() != null)
				.collect(toMap(
					entry -> GCSPartition.from(entry.getKey()),
					entry -> GCSOffset.from(entry.getValue())));
		}

		maxPoll = configGet("max.poll.records")
			.map(Integer::parseInt)
			.orElse(1000);
		gcsPollInterval = configGet("gcs.new.record.poll.interval")
			.map(Long::parseLong)
			.orElse(10_000L);
		errorBackoff = configGet("gcs.error.backoff")
			.map(Long::parseLong)
			.orElse(1000L);


		gcsSourceConfig = buildConfig(partitionNumbers);

		log.debug("{} task {} is reading from GCS with offsets {}", name(), configGet("taskNum"), offsets);

		reader = new GCSFilesReader(gcsSourceConfig, gcsClient, offsets, format::newReader).readAll();
	}

	private GCSSourceConfig buildConfig(Set<Integer> partitionNumbers) {

		String bucket = configGet("gcs.bucket").orElseThrow(() -> new ConnectException("No bucket configured!"));
		String prefix = configGet("gcs.prefix").orElse("");

		Set<String> topics = configGet("topics")
			.map(Object::toString)
			.map(s -> Arrays.stream(s.split(",")).collect(toSet()))
			.orElseGet(HashSet::new);

		Set<String> topicsToIgnore = configGet("topics.ignore")
			.map(Object::toString)
			.map(s -> Arrays.stream(s.split(",")).collect(toSet()))
			.orElseGet(HashSet::new);

		Boolean splitTopicsAcrossTasks = Boolean.parseBoolean(configGet("tasks.splitTopics").orElse("false"));

		GCSSourceConfig config = new GCSSourceConfig(
			configGet("taskNum"),
			bucket, prefix,
			configGet("gcs.page.size").map(Integer::parseInt).orElse(100),
			configGet("gcs.start.marker").orElse(null),
			GCSFilesReader.DEFAULT_PATTERN,
			GCSFilesReader.InputFilter.GUNZIP,
			GCSFilesReader.PartitionFilter.from((topic, partition) ->
				(topics.isEmpty() || topics.contains(topic))
					&& (topicsToIgnore.isEmpty() || !topicsToIgnore.contains(topic))
					&& (splitTopicsAcrossTasks || partitionNumbers.contains(partition))
					&& (!splitTopicsAcrossTasks || checkIfHashedTopicBelongsToTask(
						topic+"-"+partition
				))
			)
		);

		return config;
	}

	private Boolean checkIfHashedTopicBelongsToTask(String topicName) {
		byte[] bytesOfMessage = new byte[0];
		int taskNum = Integer.parseInt(configGet("taskNum").get());
		int taskCount = Integer.parseInt(configGet("taskCount").get());
		int hashCode = hash(topicName);
		log.debug("hashCode for {} is {}", topicName, hashCode);
		return hashCode % taskCount == taskNum;
	}

	static final int hash(Object key) {
		//https://stackoverflow.com/a/60554569/4325661
		int h;
		return (key == null) ? 0 : -(h = key.hashCode()) ^ (h >>> 16);
	}

	private Optional<String> configGet(String key) {
		return Optional.ofNullable(taskConfig.get(key));
	}


	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		// read up to the configured poll size
		List<SourceRecord> results = new ArrayList<>(maxPoll);

		if (stopped.get()) {
			return results;
		}

		// AWS errors will happen. Nothing to do about it but sleep and try again.
		while (!stopped.get()) {
			try {
				return getSourceRecords(results);
			} catch (StorageException e) {
				if (e.isRetryable()) {
					log.warn("Retryable error while polling. Will sleep and try again.", e);
					Thread.sleep(errorBackoff);
					readFromStoredOffsets();
				} else {
					// die
					throw e;
				}
			}
		}
		return results;
	}

	private List<SourceRecord> getSourceRecords(List<SourceRecord> results) throws InterruptedException {
		while (!reader.hasNext() && !stopped.get()) {
			log.debug("task {} blocking for {} ms then will parse whole bucket again.", configGet("taskNum"), gcsPollInterval);
			// TODO: sleep and block here until new files are available if posssible - by reusing iterator
			Thread.sleep(gcsPollInterval);
			readFromStoredOffsets();
		}

		if (stopped.get()) {
			return results;
		}

		for (int i = 0; reader.hasNext() && i < maxPoll && !stopped.get(); i++) {
			GCSSourceRecord record = reader.next();
			updateOffsets(record.file(), record.offset());
			String topic = topicMapping.computeIfAbsent(record.topic(), this::remapTopic);
			// we know the reader returned bytes so, we can cast the key+value and use a converter to
			// generate the "real" source record
			Optional<SchemaAndValue> key = keyConverter.map(c -> c.toConnectData(topic, record.key()));
			SchemaAndValue value = valueConverter.toConnectData(topic, record.value());
			results.add(new SourceRecord(record.file().asMap(), record.offset().asMap(), topic,
				record.partition(),
				key.map(SchemaAndValue::schema).orElse(null), key.map(SchemaAndValue::value).orElse(null),
				value.schema(), value.value(),
				null,
				record.headers()
			));
		}

		log.debug("{} task {} returning {} records.", name(), configGet("taskNum"), results.size());
		return results;
	}

	private void updateOffsets(GCSPartition file, GCSOffset offset) {
		// store the larger offset. we don't read out of order (could probably get away with always writing what we are handed)
		GCSOffset current = offsets.getOrDefault(file, offset);
		if (current.compareTo(offset) < 0) {
			log.debug("{} updated offset for {} to {}", name(), file, offset);
			offsets.put(file, offset);
		} else {
			offsets.put(file, current);
		}
	}

	@Override
	public void commit() throws InterruptedException {
		log.debug("{} task {} Commit offsets {}", name(), configGet("taskNum"), offsets);
	}

	@Override
	public void commitRecord(SourceRecord record) throws InterruptedException {
		log.debug("{} task {} Commit record w/ offset {}", name(), configGet("taskNum"), record.sourceOffset());
	}

	private String name() {
		return configGet("name").orElse("???");
	}

	private String remapTopic(String originalTopic) {
		return taskConfig.getOrDefault(CONFIG_TARGET_TOPIC + "." + originalTopic, originalTopic);
	}

	@Override
	public void stop() {
		this.stopped.set(true);
	}

}
