package com.flipkart.fdp.superbi.web.logger;

import com.flipkart.fdp.audit.dao.AuditDao;
import com.flipkart.fdp.audit.dao.DSQueryInfoDao;
import com.flipkart.fdp.audit.dao.ExecuteQueryInfoDao;
import com.flipkart.fdp.audit.dao.QueryInfoFactDao;
import com.flipkart.fdp.audit.entities.AuditInfo;
import com.flipkart.fdp.audit.entities.DSQueryInfoLog;
import com.flipkart.fdp.audit.entities.ExecutorQueryInfoLog;
import com.flipkart.fdp.audit.entities.QueryInfoFact;
import com.flipkart.fdp.dao.common.transaction.WorkUnit;
import com.flipkart.fdp.superbi.core.config.SuperbiConfig;
import com.flipkart.fdp.superbi.core.logger.impl.HystrixBasedAuditer;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.google.inject.Inject;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;

@Slf4j
public class AuditDBLogger extends HystrixBasedAuditer implements ExecutionAuditor {

    private final AuditDao auditDao;
    private final ExecuteQueryInfoDao executeQueryInfoDao;
    private final DSQueryInfoDao DSQueryInfoDao;
    private final QueryInfoFactDao queryInfoFactDao;
    private final SuperbiConfig superbiConfig;

    @Inject
    public AuditDBLogger(AuditDao auditDbDao, ExecuteQueryInfoDao executeQueryInfoDao,
        QueryInfoFactDao queryInfoFactDao,DSQueryInfoDao DSQueryInfoDao, SuperbiConfig superbiConfig) {
        super(AuditDBLogger.class.getSimpleName(), superbiConfig.getDBAuditerMaxThreads(), superbiConfig.getDBAuditerRequestTimeout());
        this.auditDao = auditDbDao;
        this.executeQueryInfoDao = executeQueryInfoDao;
        this.queryInfoFactDao = queryInfoFactDao;
        this.superbiConfig = superbiConfig;
        this.DSQueryInfoDao = DSQueryInfoDao;
    }

    @Override
    public boolean _audit(com.flipkart.fdp.superbi.core.model.AuditInfo auditInfoModel) {
        AuditInfo auditInfo = getAuditInfoEntity(auditInfoModel);
        auditDao.performInTransaction(new WorkUnit() {
            @Override
            public void perform() {
                auditDao.persist(auditInfo);
            }
        });
        return true;
    }

    private AuditInfo getAuditInfoEntity(com.flipkart.fdp.superbi.core.model.AuditInfo auditInfoModel) {
        return AuditInfo.builder()
            .dType(auditInfoModel.getClass().getSimpleName())
            .userName(auditInfoModel.getUserName())
            .methodName(auditInfoModel.getMethodName())
            .requestType(auditInfoModel.getRequestType())
            .requestTimeStamp(auditInfoModel.getRequestTimeStamp())
            .httpStatus(auditInfoModel.getHttpStatus())
            .timeTaken(auditInfoModel.getTimeTaken()).host(auditInfoModel.getHost())
            .contextEntity(auditInfoModel.getContextEntity()).traceId(auditInfoModel.getTraceId())
            .jsonPayload(auditInfoModel.getJsonPayload().toString())
            .errorMessage(auditInfoModel.getErrorMessage())
            .uri(auditInfoModel.getUri())
            .entity(auditInfoModel.getEntity())
            .org(auditInfoModel.getOrg()).namespace(auditInfoModel.getNamespace()).name(auditInfoModel.getName())
            .factName(auditInfoModel.getFactName())
            .action(auditInfoModel.getAction())
            .operationType(auditInfoModel.getOperationType().getValue())
            .requestId(auditInfoModel.getRequestId()).build();
    }

    @Override
    public boolean _audit(com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog log) {
        ExecutorQueryInfoLog executorQueryInfoLog = getExecutorQueryInfoLogEntity(log);
        executeQueryInfoDao.performInTransaction(new WorkUnit() {
            @Override
            public void perform() {
                executeQueryInfoDao.persist(executorQueryInfoLog);
            }
        });
        return true;
    }

    //Copied from Hydra
    private ExecutorQueryInfoLog getExecutorQueryInfoLogEntity(com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog log) {
        return ExecutorQueryInfoLog.builder()
            .id(log.getId())
            .sourceName(log.getSourceName())
            .sourceType(log.getSourceType().name())
            .dsQuery(StringEscapeUtils.escapeEcmaScript(log.getDsQuery()))
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

    public boolean _audit(final QueryInfo queryInfo) {
        QueryInfoFact queryInfoFact = getQueryInfoFactEntity(queryInfo);
        queryInfoFactDao.performInTransaction(new WorkUnit() {
            @Override
            public void perform() {
                queryInfoFactDao.persist(queryInfoFact);
            }
        });
        return true;
    }

    protected boolean _audit(ExecutionLog executionLog) {
        ExecutorQueryInfoLog executorQueryInfoLog = getExecutorQueryInfoLogEntity(executionLog);
        DSQueryInfoLog dsQueryInfoLog = getDSQueryInfoLogEntity(executionLog);
        executeQueryInfoDao.performInTransaction(new WorkUnit() {
            @Override
            public void perform() {
                executeQueryInfoDao.persist(executorQueryInfoLog);
                DSQueryInfoDao.persist(dsQueryInfoLog);
            }
        });
        return true;
    }

    private DSQueryInfoLog getDSQueryInfoLogEntity(ExecutionLog executionLog) {
        return DSQueryInfoLog.builder().dsQuery(executionLog.getDsQuery()).requestId(executionLog.getRequestId()).createdAt(new Date()).build();
    }


    private QueryInfoFact getQueryInfoFactEntity(QueryInfo queryInfo) {
        return QueryInfoFact.builder()
            .reportOrg(queryInfo.getReportOrg())
            .reportNamespace(queryInfo.getReportNamespace())
            .reportName(queryInfo.getReportName())
            .requestId(queryInfo.getRequestId()).cacheKey(queryInfo.getCacheKey())
            .dataCallType(queryInfo.getDataCallType().name()).build();
    }

    @Override
    public boolean isAuditorEnabled() {
        return superbiConfig.isDBAuditerEnabled();
    }

    @Override
    public void audit(ExecutionLog log) {
        if (isAuditorEnabled()) {
            new RemoteCall.Builder<Boolean>(auditerName)
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
