package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
public class ReportDataResponse {

  private final QueryResultCachedValue queryCachedResult;
  private final AttemptInfo attemptInfo;
  private final long freshAsOf;

  @Builder
  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ReportDataResponse(@JsonProperty("queryCachedResult") QueryResultCachedValue queryCachedResult,
      @JsonProperty("attemptInfo") AttemptInfo attemptInfo,
      @JsonProperty("freshAsOf") long freshAsOf
  ) {
    this.attemptInfo = attemptInfo;
    this.queryCachedResult = queryCachedResult;
    this.freshAsOf = freshAsOf;
  }
}
