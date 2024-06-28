package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.ExecutorStore.EXECUTOR_STORE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.ExecutionEventObserver;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.*;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl.DistributedLock;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.util.CacheHelperUtil;
import com.flipkart.fdp.superbi.cosmos.data.query.QueryRequestUtils;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.flipkart.fdp.superbi.cosmos.exception.*;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 19/08/14
 */

public enum ExecutorFacade {
    instance;

    private static final Logger logger = LoggerFactory.getLogger("ASYNC_POLICY_LOG");

    @LogExecTime
    public QueryResult execute(final DSQuery query, final  Map<String, String[]> params,
        Optional<ICacheClient<String,QueryResult>> cacheClientOptional,
        ExecutionEventObserver executionEventObserver) {
      return execute(query, params, cacheClientOptional, executionEventObserver, Source
          .FederationType.DEFAULT);
    }

    @LogExecTime
    public QueryResult execute(final DSQuery query, final  Map<String, String[]> params,
        Optional<ICacheClient<String,QueryResult>> cacheClientOptional,
        ExecutionEventObserver executionEventObserver, Source.FederationType federationType) {
        Timer totalTimer = new Timer().start();

        ExecutionContext context = new ExecutionContext.Builder().setDSQuery(query).setParams
            (params)
            .setCacheClient(cacheClientOptional).setExecutionEventObserver(executionEventObserver)
            .setFederationType(federationType)
            .build();
        NativeQueryTranslator translator = new NativeQueryTranslator(context);
        Executable<QueryResult> executable = ExecutorPolicyCreatorImpl.instance.getBudgetedPolicy
            (context);

        return execute(context, translator, executable, totalTimer);
    }

    @LogExecTime
    public QueryResult executeAtSpecificSource(final DSQuery query, final  Map<String, String[]> params, ExecutionEventObserver executionEventObserver, String sourceName, Source.FederationType federationType) {
        return executeAtSpecificSource(query, params, Optional.empty(), executionEventObserver, sourceName, federationType);
    }

    @LogExecTime
    public QueryResult executeAtSpecificSource(final DSQuery query, final  Map<String, String[]> params, Optional<ICacheClient<String,QueryResult>> cacheClientOptional, ExecutionEventObserver executionEventObserver, String sourceName, Source.FederationType federationType) {
        Timer totalTimer = new Timer().start();

        ExecutionContext context = new ProvidedSourceExecutionContext.Builder().setDSQuery(query).setParams
            (params)
            .setCacheClient(cacheClientOptional).setExecutionEventObserver(executionEventObserver)
            .setSource(sourceName)
            .setFederationType(federationType)
            .build();
        NativeQueryTranslator translator = new NativeQueryTranslator(context);
        Executable<QueryResult> executable = ExecutorPolicyCreatorImpl.instance.getBudgetedPolicy
            (context);

        return execute(context, translator, executable, totalTimer);
    }


    @LogExecTime
    public StreamingQueryResult executeStream(final DSQuery query, Map<String, String[]> params,
        ExecutionEventObserver executionEventObserver) {
      return executeStream(query, params, executionEventObserver, Source.FederationType.DEFAULT);
    }

    @LogExecTime
    public StreamingQueryResult executeStream(final DSQuery query, Map<String, String[]> params,
        ExecutionEventObserver executionEventObserver, Source.FederationType federationType) {
        ExecutionContext context = new ExecutionContext.Builder().setDSQuery(query)
            .setParams(params).setExecutionEventObserver(executionEventObserver)
            .setFederationType(federationType)
            .build();
        NativeQueryTranslator translator = new NativeQueryTranslator(context);
        Executable<StreamingQueryResult> executable =
            ExecutorPolicyCreatorImpl.instance.getBudgetedStreamingPolicy(context);
        Optional<StreamingQueryResult> queryResultOptional = Optional.empty();

        queryResultOptional = executable.execute(context, translator);
        return queryResultOptional.get();
    }

    @LogExecTime
    public QueryHandle executeAsync(final DSQuery query, Map<String, String[]> params,
        @NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        @NotNull ICacheClient<String, QueryResult> queryResultStore,
        @NotNull ICacheClient<String, String> distributedLockStore
    ) {
      return executeAsync(query, params, queryMetaStore, queryResultStore, distributedLockStore,
          Source.FederationType.DEFAULT);
    }

    @LogExecTime
    public QueryHandle executeAsync(final DSQuery query, Map<String, String[]> params,
        @NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        @NotNull ICacheClient<String, QueryResult> queryResultStore,
        @NotNull ICacheClient<String, String> distributedLockStore,
        @NotNull Source.FederationType federationType
    ) {
        QueryHandle queryHandle = null;
        ExecutionContext context = new ExecutionContext.Builder().setDSQuery(query)
            .setParams(params).setCacheClient(Optional.of(queryResultStore))
            .setQueryResultMetaClient(Optional.of(queryMetaStore))
            .setDistributedLockStore(Optional.of(distributedLockStore))
            .setFederationType(federationType)
            .build();

        NativeQueryTranslator translator = new NativeQueryTranslator(context);
        Executable<QueryHandle> executable = ExecutorPolicyCreatorImpl.instance.getAsyncPolicy(context);

        Optional<QueryHandle> queryResultHandleOptional = executable.execute(context, translator);
        if (queryResultHandleOptional.isPresent()) {
            queryHandle = queryResultHandleOptional.get();
            logger.debug("query submitted with handle {}", queryHandle.getHandle());
        } else {
            logger.error("unable to create handle for query {}", translator.getTranslatedQuery());
            throw new HttpException(HttpStatus.SC_NOT_FOUND, "Execution Failure: Unable to create the handle for query " +
                translator.getTranslatedQuery());
        }
        return queryHandle;
    }

    @LogExecTime
    public QueryResult getQueryResult(@NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        @NotNull ICacheClient<String, QueryResult> queryResultStore,
        QueryHandle handle) {
        QueryResult queryResult = null;
        Optional<QueryResult> queryResultOptional = QueryRequestUtils.getQueryResults(queryResultStore, handle.getHandle());

        if (queryResultOptional.isPresent()) {
            queryResult = queryResultOptional.get();
            logger.debug("query results found for handle {}", handle.getHandle());
        } else {
            logger.error("no results found for handle {}", handle.getHandle());

            {
                Optional<QueryResultMeta> queryResultMeta = queryMetaStore.get(handle.getHandle());
                logger.error("meta present {} for handle {} ", new Object[] {queryResultMeta.isPresent(), handle.getHandle()});
                if (queryResultMeta.isPresent()) {
                    logger.error("Sync error detected for result bucket and meta. Meta Info {}",
                        queryResultMeta.get());
                }
            }

            throw new HttpException(HttpStatus.SC_NOT_FOUND, "Execution Failure: No results found for the handle " +
                handle.getHandle());
        }
        return queryResult;
    }

    @LogExecTime
    public void clearStores(final DSQuery query, Map<String, String[]> params,
        @NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        @NotNull ICacheClient<String, QueryResult> queryResultStore,
        @NotNull ICacheClient<String, String> distributedLockStore) {
      clearStores(query, params, queryMetaStore, queryResultStore, distributedLockStore, Source
          .FederationType.DEFAULT);
    }

    @LogExecTime
    public void clearStores(final DSQuery query, Map<String, String[]> params,
        @NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        @NotNull ICacheClient<String, QueryResult> queryResultStore,
        @NotNull ICacheClient<String, String> distributedLockStore,
        @NotNull Source.FederationType federationType) {

        ExecutionContext context = new ExecutionContext.Builder().setDSQuery(query)
            .setParams(params).setCacheClient(Optional.of(queryResultStore))
            .setQueryResultMetaClient(Optional.of(queryMetaStore))
            .setDistributedLockStore(Optional.of(distributedLockStore))
            .setFederationType(federationType)
            .build();
        NativeQueryTranslator translator = new NativeQueryTranslator(context);

        QueryHandle queryHandle = new QueryHandle.Builder()
            .setHandle(CacheHelperUtil.createKey(translator.getTranslatedQuery(), query, context))
            .build();

        queryMetaStore.remove(queryHandle.getHandle());
        queryResultStore.remove(queryHandle.getHandle());
        distributedLockStore.remove(DistributedLock.LOCK_PREFIX + queryHandle.getHandle());
    }

    public QueryResultMeta getQueryMeta(@NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        QueryHandle handle) {
        QueryResultMeta queryResultMeta = null;
        Optional<QueryResultMeta> queryResultMetaQptional = QueryRequestUtils.getQueryMeta(queryMetaStore,
            handle.getHandle());
        if (queryResultMetaQptional.isPresent()) {
            queryResultMeta = queryResultMetaQptional.get();
            logger.debug("meta status for handle {} is {}", handle.getHandle(), queryResultMeta.getStatus());
        } else {
            logger.error("No meta found for handle {}", handle.getHandle());
            throw new HttpException(HttpStatus.SC_NOT_FOUND, "Execution Failure: No result meta found for the handle " +
                handle.getHandle());
        }
        return queryResultMeta;
    }

    public List<QueryResultMeta> getQueryMetaBulk(@NotNull ICacheClient<String, QueryResultMeta> queryMetaStore,
        List<QueryHandle> handles) {

        List<QueryResultMeta> queryResultMetas = new ArrayList<>();
        for (QueryHandle handle : handles) {
            try {
                queryResultMetas.add(getQueryMeta(queryMetaStore, handle));
            } catch (Exception qme) {
                queryResultMetas.add(null);
            }
        }
        return queryResultMetas;
    }

    @LogExecTime
    public Object getNativeQuery(final DSQuery query, Map<String, String[]> params) {
      return getNativeQuery(query, params, Source.FederationType.DEFAULT);
    }

    @LogExecTime
    public Object getNativeQuery(final DSQuery query, Map<String, String[]> params, Source.FederationType federationType) {
        ExecutionContext context = new ExecutionContext.Builder().setDSQuery(query).setParams(params)
            .setFederationType(federationType).build();
        NativeQueryTranslator translator = new NativeQueryTranslator(context);

        return translator.getTranslatedQuery();
    }

    public QueryResult execute(ExecutionContext context, NativeQueryTranslator translator,
        Executable<QueryResult> executor, Timer totalTimer) {

        Exception ex = null;
        Optional<QueryResult> queryResultOptional = Optional.empty();

        try {
            queryResultOptional = executor.execute(context, translator);
        } catch (Exception e) {
            ex = e;
        } finally {
            totalTimer.stop();
            boolean cacheHit = false;
            ResultQueryStats stats = new ResultQueryStats(totalTimer.getStartTimeMs(), totalTimer.getTimeTakenMs());

            if (queryResultOptional.isPresent()) {
                cacheHit = queryResultOptional.get().isCached();
                queryResultOptional.get().setResultQueryStats(stats);
            }


            ExecutorQueryInfoLog executorQueryInfoLog = null;
            try {
                executorQueryInfoLog = new  ExecutorQueryInfoLog.Builder()
                    .setDsQuery("TBD")
                    .setSourceName(context.getSourceName())
                    .setSourceType(context.getSourceType())
                    .setTranslatedQuery(new ObjectMapper().writeValueAsString(translator.getTranslatedQuery()))
                    .setStartTimeStampMs(totalTimer.getStartTimeMs())
                    .setTranslationTimeMs(translator.getElapsedTimeMs())
                    .setExecutionTimeMs(executor.elapsedTimeMs())
                    .setTotalTimeMs(totalTimer.getTimeTakenMs())
                    .setIsCompleted(ex == null)
                    .setCacheHit(cacheHit)
                    .setIsSlowQuery(executor.elapsedTimeMs() > context.getConfig().getSlowQueryTimeOutMs())
                    .build();
            } catch (JsonProcessingException e) {
                logger.error("Unable to audit due to following exception: " + e.getMessage());
            }

            MetaCreator.get().logExecutorQueryInfo(executorQueryInfoLog);
            stats.setQueryExecutionId(executorQueryInfoLog.getId());

            decomposeException(ex);

            if (queryResultOptional.isPresent())
                return queryResultOptional.get();
            throw new QueryExecutionException("Execution Failure: No results returned");
        }
    }

    private void decomposeException(Exception ex) {
        if (ex == null) return;
        if (ex instanceof HystrixRuntimeException) {
            HystrixRuntimeException hre = (HystrixRuntimeException) ex;
            if (hre.getFailureType() == HystrixRuntimeException.FailureType.TIMEOUT)
                throw new QueryTimeoutException("Execution Timeout: " + hre.getMessage(), ex);
        } else if (ex instanceof QueryBudgetExceededException) {
            QueryBudgetExceededException qbe = (QueryBudgetExceededException) ex;
            throw new QueryBudgetExceededException(qbe.getMessage(), qbe.getFeedback());
        }
        else if (ex instanceof QueryResourceConstraintException)
            throw new QueryResourceConstraintException(ex.getMessage());
        else if(ex instanceof QueryTimeoutException)
        {
            throw (QueryTimeoutException)ex;
        }
        throw new QueryExecutionException("Execution Failure: " + ex.getMessage(), ex);
    }

    @LogExecTime
    public boolean removeFromCache(final DSQuery query, Map<String, String[]> params,
        Optional<ICacheClient<String,QueryResult>> cacheClientOptional) {
      return removeFromCache(query, params, cacheClientOptional, Source.FederationType.DEFAULT);
    }

    @LogExecTime
    public boolean removeFromCache(final DSQuery query, Map<String, String[]> params,
        Optional<ICacheClient<String,QueryResult>> cacheClientOptional, Source.FederationType federationType) {

        ExecutionContext context = new ExecutionContext.Builder().setDSQuery(query).setParams(params)
            .setCacheClient(cacheClientOptional).setFederationType(federationType).build();
        NativeQueryTranslator translator = new NativeQueryTranslator(context);
        String cacheKey = CachePolicyExecutor.generateKey(translator.getTranslatedQuery(), context.getQuery(), context);

        return cacheClientOptional.get().remove(cacheKey);

    }

    public void refreshExecutorFor(String sourceName) {
        EXECUTOR_STORE.invalidateFor(sourceName);
    }

    public void refreshAllExecutors() {
        EXECUTOR_STORE.invalidateAll();
    }

    public AbstractDSLConfig getConfigFor(String sourceName) {
        return EXECUTOR_STORE.getFor(sourceName).config;
    }

    public AbstractDSLConfig getConfigFor(DSQuery query, Map<String, String[]> paramValues) {
        return EXECUTOR_STORE.getFor(query).config;
    }

    public Explanation explain(final DSQuery query, Map<String, String[]> params) {
        return Explanation.getFor(query, params);
    }

    public AbstractDSLConfig getConfigFor(String sourceName, Source.FederationType federationType) {
        return EXECUTOR_STORE.getFor(sourceName, federationType).config;
    }

    public AbstractDSLConfig getConfigFor(DSQuery query, Source.FederationType federationType, Map<String, String[]> paramValues) {
        return EXECUTOR_STORE.getFor(query, federationType).config;
    }

    public Explanation explain(final DSQuery query, Source.FederationType federationType, Map<String, String[]> params) {
        return Explanation.getFor(query, federationType, params);
    }
}