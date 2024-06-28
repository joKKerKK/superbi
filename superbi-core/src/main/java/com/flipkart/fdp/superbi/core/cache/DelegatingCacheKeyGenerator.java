package com.flipkart.fdp.superbi.core.cache;

import com.flipkart.fdp.superbi.core.model.DSQueryRefreshRequest;
import com.flipkart.fdp.superbi.core.model.NativeQueryRefreshRequest;
import com.flipkart.fdp.superbi.core.model.QueryRefreshRequest;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;

@Slf4j
public class DelegatingCacheKeyGenerator extends CacheKeyGenerator<QueryRefreshRequest> {
  private final DSQueryCacheKeyGenerator dsQueryCacheKeyGenerator;
  private final NativeQueryCacheKeyGenerator nativeQueryCacheKeyGenerator;

  @Inject
  public DelegatingCacheKeyGenerator(DSQueryCacheKeyGenerator dsQueryCacheKeyGenerator, NativeQueryCacheKeyGenerator nativeQueryCacheKeyGenerator) {
    this.dsQueryCacheKeyGenerator = dsQueryCacheKeyGenerator;
    this.nativeQueryCacheKeyGenerator = nativeQueryCacheKeyGenerator;
  }

  @Override
  public String getCacheKey(QueryRefreshRequest queryRefreshRequest, String sourceIdentifier) {
    if (queryRefreshRequest instanceof NativeQueryRefreshRequest) {
      return nativeQueryCacheKeyGenerator.getCacheKey((NativeQueryRefreshRequest) queryRefreshRequest, sourceIdentifier);
    } else if (queryRefreshRequest instanceof DSQueryRefreshRequest) {
      return dsQueryCacheKeyGenerator.getCacheKey((DSQueryRefreshRequest) queryRefreshRequest, sourceIdentifier);
    }
    throw new UnsupportedOperationException(MessageFormat
        .format("CacheKey can not be generated for RefreshRequest of type {0} ", queryRefreshRequest.getClass().getName()));
  }
}