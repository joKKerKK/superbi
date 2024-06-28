package com.flipkart.fdp.superbi.subscription.configurations;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PlatoMetaClientConfig {
    private String basePath;
    private String canvasApiPath;
    private String widgetApiPath;
    private CircuitBreakerProperties circuitBreakerProperties;
    private int maxConnectionsTotal;
    private int maxConnectionPerHost;
    private int requestTimeoutInMillies;
    private int pooledConnectionIdleTimeout;
    private int connectTimeout;
    private int connectionTtlInMillis;
}
