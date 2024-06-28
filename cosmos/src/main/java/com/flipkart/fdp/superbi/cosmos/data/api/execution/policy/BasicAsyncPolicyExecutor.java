package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl.Orchestrator;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.util.CacheHelperUtil;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta.QueryStatus;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import java.util.Date;
import java.util.Optional;

/**
 * Created by arun.khetarpal on 25/11/15.
 */
public class BasicAsyncPolicyExecutor<T extends QueryHandle> implements Executable<T> {
    private long elaspsedTime;
    private ICacheClient<String, QueryResultMeta> queryResultMetaStore;
    private ICacheClient<String, QueryResult> queryResultStore;

    public BasicAsyncPolicyExecutor(ICacheClient<String, QueryResultMeta> queryResultMetaStore,
                                    ICacheClient<String, QueryResult> queryResultStore) {
        this.queryResultMetaStore = queryResultMetaStore;
        this.queryResultStore = queryResultStore;
    }

    @Override
    public long elapsedTimeMs() {
        return elaspsedTime;
    }

    @Override
    public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator) {
        final Timer elapsedTimer = new Timer().start();

        String threadPool = context.getSourceName();
        Object nativeQuery = translator.getTranslatedQuery();
        DSQuery query = context.getQuery();
        long factCreatedAtTime = CacheHelperUtil.getLastFactCreatedTime(context.getFromTable());

        QueryHandle handle = new QueryHandle.Builder()
                .setHandle(CacheHelperUtil.createKey(nativeQuery, query, context))
                .setPollingFrequencyInSec(context.getConfig().getQueryPollingFrequencyInSec())
                .build();

        boolean submit = canExecuteQuery(context, translator, handle, factCreatedAtTime);

        if (submit) {
            Orchestrator orchestrator = new Orchestrator(context, translator, handle, factCreatedAtTime,
                    queryResultMetaStore, queryResultStore);
            orchestrator.orchestrate();
        }

        this.elaspsedTime = elapsedTimer.stop().getEndTimeStampMs();

        if (!submit) {
            boolean isCompleted = false;
            long executionTime = 0;

            Optional<QueryResultMeta> queryResultMeta = queryResultMetaStore.get(handle.getHandle());

            if (queryResultMeta.get().getStatus() == QueryStatus.SUCCESSFUL) {
                isCompleted = true;
                executionTime = queryResultMeta.get().getExecutionEndTime() - queryResultMeta.get().getExecutionStartTime();
            }

            ExecutorQueryInfoLog executorQueryInfoLog = new ExecutorQueryInfoLog.Builder()
                    .setDsQuery("tbd")
                    .setSourceName(context.getSourceName())
                    .setSourceType(context.getSourceType())
                    .setTranslatedQuery(translator.getTranslatedQuery().toString())
                    .setStartTimeStampMs(elapsedTimer.getStartTimeMs())
                    .setTranslationTimeMs(translator.getElapsedTimeMs())
                    .setExecutionTimeMs(executionTime)
                    .setTotalTimeMs(elapsedTimer.getTimeTakenMs())
                    .setIsCompleted(isCompleted)
                    .setCacheHit(true)
                    .setIsSlowQuery(executionTime > context.getConfig().getSlowQueryTimeOutMs())
                    .build();

            MetaCreator.get().logExecutorQueryInfo(executorQueryInfoLog);
        }

        return Optional.of((T) handle);
    }

    private boolean canExecuteQuery(ExecutionContext context, NativeQueryTranslator translator,
                                    QueryHandle handle, long factCreatedAtTime) {
        boolean executeQuery = false;

        Optional<QueryResultMeta> queryResultMeta = queryResultMetaStore.get(handle.getHandle());

        if (queryResultMeta.isPresent()) {
            /* check if results are invalid now */
            if ((!checkForVerticaResourceFailures(context, queryResultMeta.get()) && queryResultMeta.get().getStatus() == QueryStatus.FAILED)
                    || factCreatedAtTime > queryResultMeta.get().getFactCreatedAtTime()) {
                executeQuery = true;
            }
        } else {
            executeQuery = true;
        }
        return executeQuery;
    }

    private boolean checkForVerticaResourceFailures (ExecutionContext context, QueryResultMeta queryResultMeta) {

        if (context.getSourceType() == SourceType.VERTICA
                && queryResultMeta.getStatus() == QueryStatus.FAILED
                && (new Date().getTime() - queryResultMeta.getErroredAtTime() < context.getConfig().getFailedQueryTTLInSec() * 1000)) {

            String errorMessage = queryResultMeta.getErrorMessage();

            if (errorMessage.contains("Execution time exceeded run time cap of 00:10"))
                return true;
            if (errorMessage.contains("Join inner did not fit in memory"))
                return true;
            if (errorMessage.contains("inner partition did not fit in memory"))
                return true;
            if (errorMessage.contains("Insufficient resources to execute plan on pool reporting"))
                return true;
        }

        return false;
    }
}
