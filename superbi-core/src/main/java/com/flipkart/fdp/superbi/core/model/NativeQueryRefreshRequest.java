package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.models.NativeQuery;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode
public class NativeQueryRefreshRequest extends QueryRefreshRequest {

  private final String cacheKey; // Future ready if client can choose a cacheKey. This may lead to issues too if client can not generate same clientId for subsequent polls
  private final long deadLine; // Future ready if client can set a deadline
  private final NativeQuery nativeQuery;
  private Map<String, String> executionEngineLabels;
  private Map<String, Object> sinkProperties; // Future ready for selecting queue/project etc

  @Builder
  @JsonCreator
  public NativeQueryRefreshRequest(@JsonProperty("storeIdentifier") String storeIdentifier,
      @JsonProperty("fromTable") String fromTable,
      @JsonProperty("appliedFilters") Map<String, String> appliedFilters,
      @JsonProperty("cacheKey") String cacheKey, @JsonProperty("deadLine") long deadLine,
      @JsonProperty("nativeQuery") NativeQuery nativeQuery,
      @JsonProperty("executionEngineLabels") Map<String, String> executionEngineLabels,
      @JsonProperty("sinkProperties") Map<String, Object> sinkProperties) {
    super(storeIdentifier, fromTable, appliedFilters);
    this.cacheKey = cacheKey;
    this.deadLine = deadLine;
    this.nativeQuery = nativeQuery;
    this.executionEngineLabels = executionEngineLabels;
    this.sinkProperties = sinkProperties;
  }

}
