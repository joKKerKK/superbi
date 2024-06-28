package com.flipkart.fdp.superbi.cosmos.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.cosmos.meta.util.Duration;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * User: aartika
 * Date: 6/6/14
 */
public class HttpClientInitializer {

    @NotNull
    @JsonProperty
    private static Duration timeout = Duration.milliseconds(500);

    @NotNull
    @JsonProperty
    private static Duration connectionTimeout = Duration.milliseconds(500);

    @NotNull
    @JsonProperty
    private static Duration timeToLive = Duration.hours(1);

    @JsonProperty
    private static boolean cookiesEnabled = false;


    @Min(1)
    @Max(Integer.MAX_VALUE)
    @JsonProperty
    private static int maxConnections = 1024;

    @Min(1)
    @Max(Integer.MAX_VALUE)
    @JsonProperty
    private static int maxConnectionsPerRoute = 1024;

    @NotNull
    @JsonProperty
    private static Duration keepAlive = Duration.milliseconds(0);

    @Min(0)
    @Max(1000)
    private int retries = 0;

    public Duration getKeepAlive() {
        return keepAlive;
    }

    private static HttpClient instance = null;

    public static HttpClient getInstance() {
        if(instance == null) {
            initializeHttpClient();
        }
        return instance;
    }

    private static void initializeHttpClient() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout((int) connectionTimeout.toMilliseconds())
                .setConnectionRequestTimeout((int) timeout.toMilliseconds())
                .build();
        instance = HttpClientBuilder.create()
                                        .setMaxConnPerRoute(maxConnectionsPerRoute)
                                        .setMaxConnTotal(maxConnections)
                                        .setDefaultRequestConfig(requestConfig)
                                        .build();
    }
}
