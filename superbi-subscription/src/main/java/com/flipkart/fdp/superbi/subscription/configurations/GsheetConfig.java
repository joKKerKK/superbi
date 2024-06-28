package com.flipkart.fdp.superbi.subscription.configurations;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import com.flipkart.fdp.superbi.CircuitBreakerProperties;

@Data
public class GsheetConfig {
  private String folderId;
  private long gsheetExpiryInSeconds;
  private final CircuitBreakerProperties circuitBreakerProperties;

  public GsheetConfig(@JsonProperty("folderId") String folderId,
                      @JsonProperty("gsheetExpiryInSeconds") long gsheetExpiryInSeconds,
                      @JsonProperty("circuitBreakerProperties") CircuitBreakerProperties circuitBreakerProperties) {
    this.folderId = folderId;
    this.gsheetExpiryInSeconds = gsheetExpiryInSeconds;
    this.circuitBreakerProperties = circuitBreakerProperties;
  }
}