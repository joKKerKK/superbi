package com.flipkart.fdp.superbi.http.client.gringotts;

import com.flipkart.fdp.superbi.CircuitBreakerProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 24/07/19
 */
@AllArgsConstructor
@Getter
public class GringottsConfiguration {
  private final String basePath;
  private final String clientId;
  private final String clientSecret;
  private final String context;
  private final int maxConnectionsTotal;
  private final int maxConnectionPerHost;
  private final int requestTimeoutInMillies;
  private final int pooledConnectionIdleTimeout;
  private final int connectTimeout;
  private final int connectionTtlInMillis;
  private final int maxThreads;
  private final CircuitBreakerProperties circuitBreakerProperties;
}
