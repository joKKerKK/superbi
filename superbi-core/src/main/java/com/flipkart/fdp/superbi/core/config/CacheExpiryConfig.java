package com.flipkart.fdp.superbi.core.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 19/06/19
 */
@AllArgsConstructor
@Getter
public class CacheExpiryConfig {
  private final long evictEverythingBeforeTimestamp;
  private final long refreshEverythingBeforeTimestamp;
  private final long evictErrorsBeforeTimestamp;
  private final long refreshErrorsBeforeTimestamp;
}
