package com.flipkart.fdp.superbi.refresher.api.execution.status;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 18/06/19
 */

@AllArgsConstructor
@Builder
@Getter
public class QueryExecutionStats {
  private final long executionStartTimestamp;
  private final long executionEndTimeStamp;
  private final int retryCount;
}
