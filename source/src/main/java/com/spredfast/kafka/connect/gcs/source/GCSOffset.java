package com.spredfast.kafka.connect.gcs.source;

import java.util.HashMap;
import java.util.Map;

public class GCSOffset implements Comparable<GCSOffset> {

	private final String gcskey;

	private final long offset;

	public GCSOffset(String gcskey, long offset) {
		this.gcskey = gcskey;
		this.offset = offset;
	}

	public static GCSOffset from(String gcskey, long offset) {
		return new GCSOffset(gcskey, offset);
	}

	public static GCSOffset from(Map<String, Object> map) {
		return from((String) map.get("gcskey"), (Long) map.get("originalOffset"));
	}

	public String getGCSkey() {
		return gcskey;
	}

	public long getOffset() {
		return offset;
	}

	public Map<String, ?> asMap() {
		Map<String, Object> map = new HashMap<>();
		map.put("gcskey", gcskey);
		map.put("originalOffset", offset);
		return map;
	}

	@Override
	public String toString() {
		return gcskey + "@" + offset;
	}

	@Override
	public int compareTo(GCSOffset o) {
		int i = gcskey.compareTo(o.gcskey);
		return i == 0 ? (int) (offset - o.offset) : i;
	}
}
