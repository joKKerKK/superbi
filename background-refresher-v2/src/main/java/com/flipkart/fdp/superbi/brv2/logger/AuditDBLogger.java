package com.flipkart.fdp.superbi.brv2.logger;

import com.flipkart.fdp.audit.dao.ExecuteQueryInfoDao;
import com.flipkart.fdp.audit.entities.ExecutorQueryInfoLog;
import com.flipkart.fdp.dao.common.transaction.WorkUnit;
import com.flipkart.fdp.superbi.brv2.config.BRv2Config;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;

@Slf4j
public class AuditDBLogger implements ExecutionAuditor, Auditor {

  private final ExecuteQueryInfoDao executeQueryInfoDao;
  private final BRv2Config bRv2Config;
  private final String auditorName;
  private final int requestTimeoutInMillies;

  @Inject
  public AuditDBLogger(ExecuteQueryInfoDao executeQueryInfoDao, BRv2Config bRv2Config) {
    this.executeQueryInfoDao = executeQueryInfoDao;
    this.bRv2Config = bRv2Config;
    this.auditorName = AuditDBLogger.class.getSimpleName();
    this.requestTimeoutInMillies = bRv2Config.getDBAuditorRequestTimeout();
  }


  private ExecutorQueryInfoLog getExecutorQueryInfoLogEntity(ExecutionLog log) {
    return ExecutorQueryInfoLog.builder()
        .id(log.getId())
        .sourceName(log.getSourceName())
        .sourceType("NA")
        .dsQuery(null)
        .translatedQuery(StringEscapeUtils.escapeEcmaScript(log.getTranslatedQuery()))
        .startTimeStampMs(log.getStartTimeStampMs())
        .translationTimeMs(log.getTranslationTimeMs()).executionTimeMs(log.getExecutionTimeMs())
        .totalTimeMs(log.getTotalTimeMs()).isCompleted(log.isCompleted())
        .isSlowQuery(log.isSlowQuery())
        .message(StringEscapeUtils.escapeEcmaScript(log.getMessage()))
        .attempt(log.getAttemptNumber())
        .requestId(log.getRequestId())
        .factName(log.getFactName())
        .cacheHit(log.isCacheHit())
        .build();
  }


  protected boolean _audit(ExecutionLog executionLog) {
    ExecutorQueryInfoLog executorQueryInfoLog = getExecutorQueryInfoLogEntity(executionLog);
    executeQueryInfoDao.performInTransaction(new WorkUnit() {
      @Override
      public void perform() {
        executeQueryInfoDao.persist(executorQueryInfoLog);
      }
    });
    return true;
  }


  @Override
  public boolean isAuditorEnabled() {
    return bRv2Config.isAuditorEnabled();
  }

  @Override
  public void audit(ExecutionLog log) {
    if (isAuditorEnabled()) {
      new RemoteCall.Builder<Boolean>(auditorName)
          .withTimeOut(requestTimeoutInMillies)
          .around(new ActualCall<Boolean>() {
            @Override
            public Boolean workUnit() {
              return _audit(log);
            }
          }).executeAsync();
    }
  }
}
