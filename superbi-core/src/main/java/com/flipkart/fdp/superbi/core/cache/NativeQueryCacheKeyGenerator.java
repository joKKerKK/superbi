package com.flipkart.fdp.superbi.core.cache;

import com.flipkart.fdp.superbi.core.model.NativeQueryRefreshRequest;
import org.apache.commons.lang3.StringUtils;

public class NativeQueryCacheKeyGenerator extends CacheKeyGenerator<NativeQueryRefreshRequest>{
  @Override
  public String getCacheKey(NativeQueryRefreshRequest queryRefreshRequest, String sourceIdentifier) {
    // Trust client and return cacheKey sent by client
    if(StringUtils.isNotBlank(queryRefreshRequest.getCacheKey())) {
      return queryRefreshRequest.getCacheKey();
    }
    // Fallback on generating a cacheKey

    final StringBuilder cacheKey = new StringBuilder();
    cacheKey.append(queryRefreshRequest.getNativeQuery());

    return "superbi_" + sourceIdentifier + "_" + toMD5(cacheKey.toString());
  }
}