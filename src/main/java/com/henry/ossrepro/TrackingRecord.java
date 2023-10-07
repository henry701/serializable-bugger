package com.henry.ossrepro;

import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(
    toBuilder = true,
    builderMethodName = "newBuilder",
    builderClassName = "Builder",
    setterPrefix = "set")
@NoArgsConstructor
@AllArgsConstructor
public class TrackingRecord implements Serializable {

  private String id;
  private String traceId;
  private String parentTraceId;
  private String operation;
  private String version;
  private String entity;
  private List<String> steps;
  private boolean success;
  private String errorCode;
  private String errorMessage;
  private String country;
  private String vendorId;
  private String payload;
  private String sourceSystem;
  private long durationMs;
  private long createdAt;
  private boolean endSystem;

  public boolean getSuccess() {
    return this.success;
  }

  public boolean getEndSystem() {
    return this.endSystem;
  }
}
