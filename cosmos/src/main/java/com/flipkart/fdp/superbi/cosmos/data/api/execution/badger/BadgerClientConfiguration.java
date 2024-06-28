package com.flipkart.fdp.superbi.cosmos.data.api.execution.badger;

import com.flipkart.fdp.superbi.CircuitBreakerProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
public class BadgerClientConfiguration {
    private final String basePath;
    private final int port;
    private final int maxConnectionsTotal;
    private final int maxConnectionPerHost;
    private final int requestTimeoutInMillies;
    private final int pooledConnectionIdleTimeout;
    private final int connectTimeout;
    private final int connectionTtlInMillis;
    private final int maxThreads;
    private final CircuitBreakerProperties circuitBreakerProperties;
}
