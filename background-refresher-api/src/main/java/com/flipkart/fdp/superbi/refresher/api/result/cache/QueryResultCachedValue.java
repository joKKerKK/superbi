package com.flipkart.fdp.superbi.refresher.api.result.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import com.flipkart.fdp.superbi.refresher.api.result.query.RawQueryResult;
import com.flipkart.fdp.superbi.refresher.api.result.query.RawQueryResultWithSchema;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 18/06/19
 */

@Getter
public class QueryResultCachedValue implements JsonSerializable{

  private final String cacheKey;
  private final long cachedAtTime;
  private final String executingRefresherThreadId;
  private final RawQueryResult queryResult;
  private final int totalNumberOfRows;
  private final int truncatedRows;
  private final boolean truncated;
  private final String refresherNodeAddress;
  private final String d42Link;
  private final int queryLimit;

  @Builder
  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public QueryResultCachedValue(@JsonProperty("cacheKey") String cacheKey,
      @JsonProperty("cachedAtTime") long cachedAtTime,
      @JsonProperty("executingRefresherThreadId") String executingRefresherThreadId,
      @JsonProperty("queryResult") RawQueryResult queryResult,
      @JsonProperty("totalNumberOfRows") int totalNumberOfRows,
      @JsonProperty("truncatedRows") int truncatedRows,
      @JsonProperty("truncated") boolean truncated,
      @JsonProperty("refresherNodeAddress") String refresherNodeAddress,
      @JsonProperty("d42Link") String d42Link,
      @JsonProperty("queryLimit") int queryLimit
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
    this.queryLimit = queryLimit;
  }

  @JsonIgnore
  public RawQueryResult getValue() {
    return queryResult;
  }

  public QueryResultCachedValue copy(RawQueryResult rawQueryResult, Schema schema) {
    RawQueryResultWithSchema queryResultWithSchema = RawQueryResultWithSchema.withSchemaBuilder()
        .schema(schema)
        .rawQueryResult(rawQueryResult).build();
    return new QueryResultCachedValue(this.cacheKey, this.cachedAtTime,
        this.executingRefresherThreadId, queryResultWithSchema,
        this.totalNumberOfRows, this.truncatedRows, this.truncated, this.refresherNodeAddress,this.d42Link
    ,this.queryLimit);
  }
}
