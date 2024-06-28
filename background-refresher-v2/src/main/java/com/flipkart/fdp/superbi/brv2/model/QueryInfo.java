package com.flipkart.fdp.superbi.brv2.model;

import lombok.Builder;
import lombok.Data;

/**
 * Created by akshaya.sharma on 29/08/19
 */
@Data
@Builder
public class QueryInfo {
  public enum DATA_CALL_TYPE {POLL, REFRESH};

  private String requestId;
  private String reportOrg;
  private String reportNamespace;
  private String reportName;
  private String queryExecutionId;
  private String cacheKey;
  private DATA_CALL_TYPE dataCallType;
}
