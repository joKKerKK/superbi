package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryResultCachedValue {

  private final String cacheKey;
  private final long cachedAtTime;
  private final String executingRefresherThreadId;
  private final RawQueryResultWithSchema queryResult;
  private final int totalNumberOfRows;
  private final int truncatedRows;
  private final boolean truncated;
  private final String refresherNodeAddress;
  private final String d42Link;

  @Builder
  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public QueryResultCachedValue(@JsonProperty("cacheKey") String cacheKey,
      @JsonProperty("cachedAtTime") long cachedAtTime,
      @JsonProperty("executingRefresherThreadId") String executingRefresherThreadId,
      @JsonProperty("queryResult") RawQueryResultWithSchema queryResult,
      @JsonProperty("totalNumberOfRows") int totalNumberOfRows,
      @JsonProperty("truncatedRows") int truncatedRows,
      @JsonProperty("truncated") boolean truncated,
      @JsonProperty("refresherNodeAddress") String refresherNodeAddress,
      @JsonProperty("d42Link") String d42Link
  ) {
    this.cacheKey = cacheKey;
    this.cachedAtTime = cachedAtTime;
    this.executingRefresherThreadId = executingRefresherThreadId;
    this.queryResult = queryResult;
    this.totalNumberOfRows = totalNumberOfRows;
    this.truncatedRows = truncatedRows;
    this.truncated = truncated;
    this.refresherNodeAddress = refresherNodeAddress;
    this.d42Link = d42Link;
  }

  @JsonIgnore
  public RawQueryResultWithSchema getValue() {
    return queryResult;
  }

}
