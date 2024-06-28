package com.flipkart.fdp.superbi.core.model;

import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public class QueryResultInfo {
  private final String cacheKey;
  private final AttemptInfo attemptInfo;
  private final QueryResultCachedValue queryCachedResult;
  private final long currentTime;
  private final long factRefreshedAtTime;
  private final long freshAsOf;
  private final boolean queryLocked;
  private final boolean refreshRequired;
}