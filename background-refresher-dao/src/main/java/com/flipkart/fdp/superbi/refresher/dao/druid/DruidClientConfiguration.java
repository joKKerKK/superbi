package com.flipkart.fdp.superbi.refresher.dao.druid;

import com.flipkart.fdp.superbi.CircuitBreakerProperties;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DruidClientConfiguration {
  private final String basePath;
  private final int port;
  private final int maxConnectionsTotal;
  private final int maxConnectionPerHost;
  private final int requestTimeoutInMillies;
  private final int readTimeoutInMillies;
  private final int pooledConnectionIdleTimeout;
  private final int connectTimeout;
  private final int connectionTtlInMillis;
  private final int maxThreads;
  private final CircuitBreakerProperties circuitBreakerProperties;
  private final List<String> clientSideExceptions;
}
