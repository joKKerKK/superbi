package com.flipkart.fdp.superbi.refresher.api.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 19/06/19
 */
@AllArgsConstructor
@Getter
public class BackgroundRefresherConfig {

  private final long delayInLockAndExecuteInMs;
  private final long queryTimeoutInMs;
  private final int refreshIntervalInSec;
  private final int errorRefreshIntervalInSec;
  private final int resultCacheTtlInSec;
  private final int attemptInfoTtlInSecForClientSideException;
  private final int numOfRetryOnException;
  private final long retryOnExceptionBackoffInMillis;
  private final long responseTruncationSizeInBytes;
  private final double brv2TrafficFactor;
  private final long d42MaxSizeInMB;

  public long getLockTimeoutInMillies() {
    return queryTimeoutInMs + delayInLockAndExecuteInMs;
  }

  //If TTL for Client side exception is not set. Default is 24 hours.
  public int getAttemptInfoTtlInSecForClientSideException() {
    return attemptInfoTtlInSecForClientSideException > 0 ? attemptInfoTtlInSecForClientSideException
        : 60 * 10;
  }

  public long getD42MaxSizeInMB() {
    return d42MaxSizeInMB > 0 ? d42MaxSizeInMB : 1024;
  }

  public long getResponseTruncationSizeInBytes() {
    return responseTruncationSizeInBytes > 0 ? responseTruncationSizeInBytes : 10 * 1024 * 1024L;
  }

  public double getBrv2TrafficFactor() {
    return brv2TrafficFactor > 0.0d ? brv2TrafficFactor : 0.0d;
  }
}
