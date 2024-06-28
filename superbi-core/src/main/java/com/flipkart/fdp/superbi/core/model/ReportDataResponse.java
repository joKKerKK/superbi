package com.flipkart.fdp.superbi.core.model;

import com.codahale.metrics.annotation.Metered;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 18/06/19
 */

@Getter
public class ReportDataResponse extends FetchQueryResponse {

  private final QueryResultCachedValue queryCachedResult;
  private final AttemptInfo attemptInfo;
  private final long freshAsOf;
  private final String cacheKey;

  @JsonIgnore
  private final QueryInfo.DATA_CALL_TYPE dataCallType;

  @Builder
  @Metered
  public ReportDataResponse(Map<String, String> appliedFilters,
      QueryResultCachedValue queryCachedResult,
      AttemptInfo attemptInfo, long freshAsOf, String cacheKey,
      QueryInfo.DATA_CALL_TYPE dataCallType) {
    super(appliedFilters);
    this.queryCachedResult = queryCachedResult;
    this.attemptInfo = attemptInfo;
    this.freshAsOf = freshAsOf;
    this.dataCallType = dataCallType;
    this.cacheKey = cacheKey;
  }
}
