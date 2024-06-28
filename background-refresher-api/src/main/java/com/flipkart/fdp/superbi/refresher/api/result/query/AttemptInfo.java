package com.flipkart.fdp.superbi.refresher.api.result.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * Created by akshaya.sharma on 18/06/19
 */
@Getter
public class AttemptInfo implements JsonSerializable {

  private final long finishedAtTime;

  private final Status status;

  private final long startedAtTime;

  private final String errorMessage;

  private final boolean serverError;

  private final long cachedAtTime;

  private final Map<String, String> _debugInfo;

  private final String attemptKey;

  private final String cacheKey;

  private final String requestId;

  private final String refresherNodeAddress;

  @Builder
  @JsonCreator
  public AttemptInfo(@JsonProperty("finishedAtTime") long finishedAtTime,
      @JsonProperty("status") Status status,
      @JsonProperty("startedAtTime") long startedAtTime,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("serverError") boolean serverError,
      @JsonProperty("cachedAtTime") long cachedAtTime,
      @JsonProperty("_debugInfo") Map<String, String> _debugInfo,
      @JsonProperty("attemptKey") String attemptKey,
      @JsonProperty("cacheKey") String cacheKey,
      @JsonProperty("requestId") String requestId,
      @JsonProperty("refresherNodeAddress") String refresherNodeAddress) {
    this.finishedAtTime = finishedAtTime;
    this.status = status;
    this.startedAtTime = startedAtTime;
    this.errorMessage = errorMessage;
    this.serverError = serverError;
    this.cachedAtTime = cachedAtTime;
    this._debugInfo = _debugInfo;
    this.cacheKey = cacheKey;
    this.attemptKey = attemptKey;
    this.requestId = requestId;
    this.refresherNodeAddress = refresherNodeAddress;
  }

  @JsonIgnore
  public boolean isSuccessful() {
    return status == Status.SUCCESS;
  }

  public static enum Status {
    SUCCESS, FAILED
  }
}
