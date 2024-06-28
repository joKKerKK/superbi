package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttemptInfo{

  private final long finishedAtTime;

  private final Status status;

  private final long startedAtTime;

  private final String errorMessage;

  private final boolean serverError;

  private final long cachedAtTime;

  private final Map<String, String> _debugInfo;

  @Builder
  @JsonCreator
  public AttemptInfo(@JsonProperty("finishedAtTime") long finishedAtTime,
      @JsonProperty("status") Status status,
      @JsonProperty("startedAtTime") long startedAtTime,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("serverError") boolean serverError,
      @JsonProperty("cachedAtTime") long cachedAtTime,
      @JsonProperty("_debugInfo") Map<String, String> _debugInfo) {
    this.finishedAtTime = finishedAtTime;
    this.status = status;
    this.startedAtTime = startedAtTime;
    this.errorMessage = errorMessage;
    this.serverError = serverError;
    this.cachedAtTime = cachedAtTime;
    this._debugInfo = _debugInfo;
  }

  @JsonIgnore
  public boolean isSuccessful() {
    return status == Status.SUCCESS;
  }

  public static enum Status {
    SUCCESS, FAILED
  }
}
