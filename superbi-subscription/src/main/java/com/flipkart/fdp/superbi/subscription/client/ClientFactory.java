package com.flipkart.fdp.superbi.subscription.client;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import com.flipkart.fdp.superbi.subscription.configurations.PlatoMetaClientConfig;
import com.flipkart.fdp.superbi.subscription.configurations.SuperbiClientConfig;
import org.asynchttpclient.AsyncHttpClient;

public class ClientFactory {

  public static AsyncHttpClient getSuperbiClient(SuperbiClientConfig clientConfig){
    return asyncHttpClient(config()
        .setMaxConnections(clientConfig.getMaxConnectionsTotal())
        .setMaxConnectionsPerHost(clientConfig.getMaxConnectionPerHost())
        .setPooledConnectionIdleTimeout(clientConfig.getPooledConnectionIdleTimeout())
        .setConnectionTtl(clientConfig.getConnectionTtlInMillis())
        .setConnectTimeout(clientConfig.getConnectTimeout())
        .setRequestTimeout(clientConfig.getRequestTimeoutInMillies())
    );
  }

  public static AsyncHttpClient getPlatoMetaClient(PlatoMetaClientConfig clientConfig){
    return asyncHttpClient(config()
            .setMaxConnections(clientConfig.getMaxConnectionsTotal())
            .setMaxConnectionsPerHost(clientConfig.getMaxConnectionPerHost())
            .setPooledConnectionIdleTimeout(clientConfig.getPooledConnectionIdleTimeout())
            .setConnectionTtl(clientConfig.getConnectionTtlInMillis())
            .setConnectTimeout(clientConfig.getConnectTimeout())
            .setRequestTimeout(clientConfig.getRequestTimeoutInMillies())
    );
  }

}
