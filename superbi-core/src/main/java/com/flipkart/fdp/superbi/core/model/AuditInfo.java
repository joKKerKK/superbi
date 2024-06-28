package com.flipkart.fdp.superbi.core.model;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AuditInfo {

  private Date createdAt;
  private Date updatedAt;
  private String methodName;
  private String requestType;
  private String userName;
  private String action;
  private String entity;
  private String org;
  private String namespace;
  private String name;
  private OperationType operationType;
  private String uri;
  private String jsonPayload;
  private long requestTimeStamp;
  private int httpStatus;
  private long timeTaken;
  private String host;
  private String contextEntity;
  private String traceId;
  private String requestId;
  private String errorMessage;
  private String factName;
}
