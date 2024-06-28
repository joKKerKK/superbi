package com.flipkart.fdp.superbi.d42;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.CircuitBreakerProperties;
import lombok.Builder;
import lombok.Getter;

@Getter
public class D42Configuration {
    private final String d42Endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private final CircuitBreakerProperties circuitBreakerProperties;
    private final String projectId;

    @JsonCreator
    @Builder
    public D42Configuration(@JsonProperty("d42Endpoint") String d42Endpoint,
        @JsonProperty("accessKey")
            String accessKey, @JsonProperty("secretKey") String secretKey,
        @JsonProperty("bucket") String bucket,
        @JsonProperty("circuitBreakerProperties") CircuitBreakerProperties circuitBreakerProperties,
        @JsonProperty("projectId")String projectId) {
        this.d42Endpoint = d42Endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.circuitBreakerProperties = circuitBreakerProperties;
        this.projectId = projectId;
    }
}
