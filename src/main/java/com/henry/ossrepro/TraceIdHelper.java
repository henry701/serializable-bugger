package com.henry.ossrepro;

import static java.util.Objects.nonNull;

public class TraceIdHelper {

  private TraceIdHelper() {
  }

  private static final String PART_TAG = "-part-";

  private static final int NOT_FOUND = -1;

  private static final int START_INDEX = 0;

  public static String getTraceIdFromChunk(String traceIdChunk) {
    int tagIndex = nonNull(traceIdChunk) ? traceIdChunk.indexOf(PART_TAG) : NOT_FOUND;
    return tagIndex > NOT_FOUND ? traceIdChunk.substring(START_INDEX, tagIndex) : traceIdChunk;
  }

}
