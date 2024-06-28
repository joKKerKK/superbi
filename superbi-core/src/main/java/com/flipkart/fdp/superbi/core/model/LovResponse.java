package com.flipkart.fdp.superbi.core.model;

import com.codahale.metrics.annotation.Metered;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
public class LovResponse extends FetchQueryResponse{
  private final QueryResultCachedValue queryCachedResult;
  private final AttemptInfo attemptInfo;
  private final boolean highCardinality;

  @Builder
  @Metered
  public LovResponse(Map<String, String> appliedFilters,
      QueryResultCachedValue queryCachedResult,
      AttemptInfo attemptInfo, boolean highCardinality) {
    super(appliedFilters);
    this.queryCachedResult = queryCachedResult;
    this.attemptInfo = attemptInfo;
    this.highCardinality = highCardinality;
  }
}
