package com.flipkart.fdp.superbi.brv2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.compito.api.request.Message;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SuperBiMessage implements Message<SuperBiMessage> {

  private final long submittedAt;
  private final int remainingRetries;
  private final long executeAfter;
  private final long backoffInMillis;
  private final QueryPayload queryPayload;
  private final ExecutionState executionState;


  @JsonCreator
  public SuperBiMessage(@JsonProperty("submittedAt") long submittedAt,
      @JsonProperty("remainingRetries") int remainingRetries,
      @JsonProperty("executeAfter") long executeAfter,
      @JsonProperty("backoffInMillis") long backoffInMillis,
      @JsonProperty("queryPayload") QueryPayload queryPayload,
      @JsonProperty("executionState") ExecutionState executionState) {
    this.submittedAt = submittedAt;
    this.remainingRetries = remainingRetries;
    this.queryPayload = queryPayload;
    this.executeAfter = executeAfter;
    this.backoffInMillis = backoffInMillis;
    this.executionState = executionState;
  }

  public SuperBiMessage(int remainingRetries, long executeAfter, long backoffInMillis,
      QueryPayload queryPayload) {
    this(System.currentTimeMillis(), remainingRetries, executeAfter, backoffInMillis, queryPayload,
        ExecutionState.SUBMITTED);
  }

  @Override
  @JsonIgnore
  public long getExecuteBefore() {
    return queryPayload.getDeadLine() + System.currentTimeMillis();
  }

  @Override
  @JsonIgnore
  public long getCost() {
    return queryPayload.getQueryWeight();
  }

  @Override
  public SuperBiMessage withRemainingRetries(int remainingRetries) {
    return new SuperBiMessage(submittedAt, remainingRetries, executeAfter, backoffInMillis,
        queryPayload, executionState);
  }

  @Override
  public SuperBiMessage withExecuteAfter(long executeAfter) {
    return new SuperBiMessage(submittedAt, remainingRetries, executeAfter, backoffInMillis,
        queryPayload, executionState);
  }

  @Override
  public SuperBiMessage withExecutionState(ExecutionState executionState) {
    return new SuperBiMessage(submittedAt, remainingRetries, executeAfter, backoffInMillis,
        queryPayload, executionState);
  }

  @Override
  public SuperBiMessage withExecuteBefore(long executeBefore) {
    QueryPayload newQueryPayload = QueryPayload.builder()
        .attemptKey(queryPayload.getAttemptKey())
        .cacheKey(queryPayload.getCacheKey())
        .clientId(queryPayload.getClientId())
        .dsQuery(queryPayload.getDsQuery())
        .params(queryPayload.getParams())
        .storeIdentifier(queryPayload.getStoreIdentifier())
        .dateRange(queryPayload.getDateRange())
        .deadLine(executeBefore)
        .priority(queryPayload.getPriority())
        .queryWeight(queryPayload.getQueryWeight())
        .requestId(queryPayload.getRequestId())
        .nativeQuery(queryPayload.getNativeQuery())
        .metaDataPayload(queryPayload.getMetaDataPayload())
        .build();
    return new SuperBiMessage(submittedAt, remainingRetries, executeAfter, backoffInMillis,
        newQueryPayload, executionState);
  }
}
