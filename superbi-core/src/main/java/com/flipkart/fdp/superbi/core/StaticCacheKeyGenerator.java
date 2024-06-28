package com.flipkart.fdp.superbi.core;

import com.flipkart.fdp.superbi.core.cache.CacheKeyGenerator;
import com.flipkart.fdp.superbi.core.model.QueryRefreshRequest;
import lombok.AllArgsConstructor;

/**
 * Created by akshaya.sharma on 08/07/19
 */

@AllArgsConstructor
public class StaticCacheKeyGenerator extends CacheKeyGenerator<QueryRefreshRequest> {
  private final String cacheKey;

  @Override
  public String getCacheKey(QueryRefreshRequest queryRefreshRequest, String sourceIdentifier) {
    return cacheKey;
  }
}
