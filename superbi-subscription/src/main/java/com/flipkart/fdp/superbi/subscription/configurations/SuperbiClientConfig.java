package com.flipkart.fdp.superbi.subscription.configurations;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Getter;


@Getter
public class SuperbiClientConfig {

  private final String basePath;
  private final String dataCallPath;
  private final Map<String,ApiAuthConfig> apiAuthConfigMap;
  private final CircuitBreakerProperties circuitBreakerProperties;
  private final int maxConnectionsTotal;
  private final int maxConnectionPerHost;
  private final int requestTimeoutInMillies;
  private final int pooledConnectionIdleTimeout;
  private final int connectTimeout;
  private final int connectionTtlInMillis;


  @JsonCreator
  public SuperbiClientConfig(@JsonProperty("basePath") String basePath,
      @JsonProperty("dataCallPath") String dataCallPath, @JsonProperty("clientId") Map<String,ApiAuthConfig> apiAuthConfigMap,
      @JsonProperty("circuitBreakerProperties") CircuitBreakerProperties circuitBreakerProperties,
      @JsonProperty("maxConnectionsTotal") int maxConnectionsTotal,
      @JsonProperty("maxConnectionPerHost") int maxConnectionPerHost,
      @JsonProperty("requestTimeoutInMillies") int requestTimeoutInMillies, @JsonProperty("pooledConnectionIdleTimeout")int pooledConnectionIdleTimeout,
      @JsonProperty("connectTimeout") int connectTimeout, @JsonProperty("connectionTtlInMillis") int connectionTtlInMillis) {
    this.basePath = basePath;
    this.dataCallPath = dataCallPath;
    this.apiAuthConfigMap = apiAuthConfigMap;
    this.circuitBreakerProperties = circuitBreakerProperties;
    this.maxConnectionsTotal = maxConnectionsTotal;
    this.maxConnectionPerHost = maxConnectionPerHost;
    this.requestTimeoutInMillies = requestTimeoutInMillies;
    this.pooledConnectionIdleTimeout = pooledConnectionIdleTimeout;
    this.connectTimeout = connectTimeout;
    this.connectionTtlInMillis = connectionTtlInMillis;
  }
}
