package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.google.common.base.Optional;
import lombok.*;

/**
 * Created by mansi.jain on 21/02/22
 */
@Builder
@Getter
public class BigQueryClientConfig {
  private String projectId;
  private Optional<RetryConfig> clientRetryConfig;

  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  @ToString
  public static class RetryConfig {
    private Optional<Integer> maxAttempts;
    private Optional<Long> initialRetryDelayMs;
    private Optional<Double> retryDelayMultiplier;
    private Optional<Long> maxRetryDelayMs;
    private Optional<Long> initialRpcTimeoutMs;
    private Optional<Double> rpcTimeoutMultiplier;
    private Optional<Long> maxRpcTimeoutMs;
    private Optional<Long> totalTimeoutMs;
  }
}
