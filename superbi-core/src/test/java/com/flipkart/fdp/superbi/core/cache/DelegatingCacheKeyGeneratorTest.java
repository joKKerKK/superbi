package com.flipkart.fdp.superbi.core.cache;

import com.flipkart.fdp.superbi.core.model.DSQueryRefreshRequest;
import com.flipkart.fdp.superbi.core.model.NativeQueryRefreshRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DelegatingCacheKeyGeneratorTest {

  public static final String SOURCE_IDENTIFIER = "SOURCE_IDENTIFIER";
  private DSQueryCacheKeyGenerator dsQueryCacheKeyGenerator;
  private NativeQueryCacheKeyGenerator nativeQueryCacheKeyGenerator;
  private DelegatingCacheKeyGenerator delegatingCacheKeyGenerator;

  @Before
  public void setUp() {
    dsQueryCacheKeyGenerator = Mockito.mock(DSQueryCacheKeyGenerator.class);
    nativeQueryCacheKeyGenerator = Mockito.mock(NativeQueryCacheKeyGenerator.class);
    delegatingCacheKeyGenerator = new DelegatingCacheKeyGenerator(dsQueryCacheKeyGenerator,
        nativeQueryCacheKeyGenerator);
  }

  @Test
  public void testGetCacheKeyForNativeQuery() {
    NativeQueryRefreshRequest nativeQueryRefreshRequest = Mockito.mock(
        NativeQueryRefreshRequest.class);
    delegatingCacheKeyGenerator.getCacheKey(nativeQueryRefreshRequest, SOURCE_IDENTIFIER);

    Mockito.verify(nativeQueryCacheKeyGenerator, Mockito.times(1))
        .getCacheKey(Mockito.any(), Mockito.any());
    Mockito.verify(dsQueryCacheKeyGenerator, Mockito.times(0))
        .getCacheKey(Mockito.any(), Mockito.any());
  }

  @Test
  public void testGetCacheKeyForDSQuery() {
    DSQueryRefreshRequest dsQueryRefreshRequest = Mockito.mock(DSQueryRefreshRequest.class);
    delegatingCacheKeyGenerator.getCacheKey(dsQueryRefreshRequest, SOURCE_IDENTIFIER);

    Mockito.verify(nativeQueryCacheKeyGenerator, Mockito.times(0))
        .getCacheKey(Mockito.any(), Mockito.any());
    Mockito.verify(dsQueryCacheKeyGenerator, Mockito.times(1))
        .getCacheKey(Mockito.any(), Mockito.any());
  }
}