package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class BigQueryJobConfig {

  private final long totalTimeoutLimitMs;

  private final boolean legacySql;

  private final Long tableSizeLimitInMbs;
}
