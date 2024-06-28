package com.flipkart.fdp.superbi.refresher.api.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.models.NativeQuery;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 19/06/19
 */

@Getter
@Builder
@EqualsAndHashCode
public class QueryPayload {

  @Getter(AccessLevel.NONE)
  private final String DEFAULT_PRIORITY = "default";

  /**
   * DEFAULT_QUERY_WAIT_TIME in milliseconds to wait after querySubmission to BackgroundRefresher,
   * before execution is started by process. 24 hours
   */
  @Getter(AccessLevel.NONE)
  private static final long DEFAULT_QUERY_WAIT_TIME = 24 * 60 * 60 * 1000;

  private final String storeIdentifier;
  private final String attemptKey;
  private final String cacheKey;
  private final long deadLine;
  @NonNull
  private final NativeQuery nativeQuery;
  private long queryWeight;
  private final String priority;
  private final String clientId;
  private final String requestId;
  /**
   * Backward compatibility
   * Pass dsQuery as null if executing only native query.
   * @return
   */
  private final DSQuery dsQuery;
  private final Map<String, String[]> params;
  private final Map<String, String> dateRange;
  private final MetaDataPayload metaDataPayload;


  @JsonCreator
  public QueryPayload(@JsonProperty("storeIdentifier") String storeIdentifier,
      @JsonProperty("attemptKey") String attemptKey,
      @JsonProperty("cacheKey") String cacheKey,
      @JsonProperty("deadLine") long deadLine,
      @JsonProperty("nativeQuery") NativeQuery nativeQuery,
      @JsonProperty("queryWeight") long queryWeight,
      @JsonProperty("priority") String priority,
      @JsonProperty("clientId") String clientId,
      @JsonProperty("requestId") String requestId,
      @JsonProperty("dsQuery") DSQuery dsQuery,
      @JsonProperty("params") Map<String, String[]> params,
      @JsonProperty("dateRange") Map<String, String> dateRange,
      @JsonProperty("metaDataPayload") MetaDataPayload metaDataPayload) {
    this.storeIdentifier = storeIdentifier;
    this.attemptKey = attemptKey;
    this.cacheKey = cacheKey;
    this.deadLine = deadLine;
    this.nativeQuery = nativeQuery;
    this.queryWeight = queryWeight;
    this.priority = priority;
    this.clientId = clientId;
    this.requestId = requestId;
    this.dsQuery = dsQuery;
    this.params = params;
    this.dateRange = dateRange;
    this.metaDataPayload = metaDataPayload;
  }

  public long getDeadLine() {
    return deadLine > 0 ? deadLine : DEFAULT_QUERY_WAIT_TIME;
  }

  public long getQueryWeight() {
    return queryWeight > 1 ? queryWeight : 1;
  }

  public String getPriority() {
    return StringUtils.isNotBlank(priority) ? priority : DEFAULT_PRIORITY;
  }

  @Override
  public String toString() {
    return "QueryPayload{" +
        "DEFAULT_PRIORITY='" + DEFAULT_PRIORITY + '\'' +
        ", storeIdentifier='" + storeIdentifier + '\'' +
        ", cacheKey='" + cacheKey + '\'' +
        ", deadLine=" + deadLine +
        ", queryWeight=" + queryWeight +
        ", priority='" + priority + '\'' +
        ", clientId='" + clientId + '\'' +
        ", dsQuery=" + dsQuery +
        ", params=" + params +
        '}';
  }
}