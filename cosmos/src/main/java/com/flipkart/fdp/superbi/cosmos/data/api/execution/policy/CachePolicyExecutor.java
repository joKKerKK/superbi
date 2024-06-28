package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.util.CacheHelperUtil;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.flipkart.fdp.superbi.cosmos.exception.CachedException;
import com.flipkart.fdp.superbi.cosmos.exception.QueryResourceConstraintException;
import com.flipkart.fdp.superbi.cosmos.exception.QueryTimeoutException;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Created by arun.khetarpal on 17/09/15.
 */
public class CachePolicyExecutor<T extends QueryResult> implements Executable<T> {
    private static final Logger cacheLogger = LoggerFactory.getLogger("CACHE_LOG");
    private static final String cacheLoggerformatString = "class=%s msg=%s";
    private final String CACHE_INTERNAL_PLACEHOLDER = "__CacheInternalPlaceHolder__";
    private Executable<T> decoratedExecutable;
    private ICacheClient<String, T> cacheClient;
    private long cacheKeyTTLInSec;
    private long elapsedTime;

    public CachePolicyExecutor(ICacheClient<String, T> cacheClient, long cacheKeyTTLInSec,
                               Executable<T> decoratedExecutable) {
        this.decoratedExecutable = decoratedExecutable;
        this.cacheClient = cacheClient;
        this.cacheKeyTTLInSec = cacheKeyTTLInSec;
    }

    @Override
    public long elapsedTimeMs() {
        return elapsedTime;
    }

    @Override
    @LogExecTime
    public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator) {
        final Timer elapsedTimer = new Timer().start();

        Object nativeQuery = translator.getTranslatedQuery();
        DSQuery query = context.getQuery();
        String fromTable = context.getFromTable();
        boolean cacheHit = false;

        long factCreatedAtTime = 0;

        {
            Date lastRefreshDate = MetaAccessor.get().getTableByName(fromTable.split("\\.").length == 1 ?
                    MetaAccessor.get().getFactByName(fromTable).getTableName() : fromTable)
                    .getLastRefresh();

            if (lastRefreshDate != null)
                factCreatedAtTime = lastRefreshDate.getTime();
        }

        String key = CacheHelperUtil.createKey(nativeQuery, query, context);

        checkIfCacheHasErrorsAndThrow(cacheClient, key);

        try {
            Optional<T> result = getFromCache(cacheClient, key, factCreatedAtTime, context.getConfig().getCachePollingTimeMs());
            cacheHit = result.isPresent() && !result.get().getHasFailed();

            if (!result.isPresent() /* cache miss */) {

                cacheLogger.info("Cache miss, going to execute the query in Datastore");
                QueryResult placeHolderResult = new QueryResult(null, null);

                placeHolderResult.setFactName(CACHE_INTERNAL_PLACEHOLDER);

                if (context.getConfig().usePlaceHolder())
                    cacheClient.set(key, (int) context.getConfig().getQueryTimeOutMs() / 1000, (T) placeHolderResult);

                result = decoratedExecutable.execute(context, translator);


                if (result.isPresent()) {
                    result.get().setFactName(fromTable);
                    result.get().setFactCreatedAtTime(factCreatedAtTime);
                    result.get().setCached(cacheHit);
                    result.get().setCachedAtTime(System.currentTimeMillis());
                    result.get().setOriginalExecutorRequestId(MDC.get("X-Request-Id"));
                    //TODO workaround to not cache vertica empty results as vertica is somehow returning empty results when its too busy
                    if(context.getSourceType()== SourceType.VERTICA && result.get().data.isEmpty())
                    {
                        cacheLogger.info(String.format("Empty data received from Vertica, factName=%s, key(hash code)=%s, factCreateTime=%s,",
                                context.getFromTable(), key, new Date(factCreatedAtTime)));
                        cacheClient.remove(key);
                    }
                    else {
                        if (!cacheClient.set(key, (int) getCacheKeyTTLInSec(context), result.get()))
                        {
                            cacheClient.remove(key);
                            cacheLogger.info(String.format("Unable to to put result data to cache for the key %s", key));
                        }
                        else
                        {
                            cacheLogger.info(String.format("Updated result data to cache successfully for the key %s", key));
                        }
                    }
                } else {
                    cacheClient.remove(key);
                }
            } else if (context.getSourceType() == SourceType.VERTICA && result.get().getHasFailed() && result.get().getErrorMessage() != null) {
                cacheLogger.info("Cache hit, but query failed error because of vertica issues");
                String exceptionMessage = String.format("The query had failed recently due to resource shortage at vertica.\n" +
                        "Please try again after sometime.");
                throw new QueryResourceConstraintException(exceptionMessage);
            }
            else {
                cacheLogger.info(String.format("Cache hit, returning the cached data, cached by RequestId=%s", result.get().originalExecutorRequestId));
                result.get().setCached(cacheHit);
            }

            elapsedTimer.stop();
            elapsedTime = elapsedTimer.getTimeTakenMs();
            if(result.isPresent()) {
                result.get().setCacheKey(key);
            }

            return result;
        } catch (Exception ex) {
                QueryResult failedPlaceHolderResult = new QueryResult(null, null);
                failedPlaceHolderResult.setErroredAtTime(new Date().getTime());
                failedPlaceHolderResult.setErrorMessage(ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
                failedPlaceHolderResult.setHasFailed();
                failedPlaceHolderResult.setExceptionStack(ExceptionUtils.getStackTrace(ex));
            if(ex instanceof HystrixRuntimeException)
            {
                HystrixRuntimeException hystrixRuntimeException = (HystrixRuntimeException) ex;
                failedPlaceHolderResult.errorType = hystrixRuntimeException.getFailureType();
            }

            String errorMessage = failedPlaceHolderResult.getErrorMessage();
            if (errorMessage != null && (errorMessage.contains("Execution time exceeded run time cap")
                || errorMessage.contains("Join inner did not fit in memory")
                || errorMessage.contains("inner partition did not fit in memory")
                || errorMessage.contains("Insufficient resources to execute plan on pool reporting")
                || (errorMessage.contains("ElasticSearch failed") && errorMessage.contains
                ("circuit_breaking_exception") && errorMessage.contains("Data too large")))
                || (failedPlaceHolderResult.errorType != null && failedPlaceHolderResult.errorType == HystrixRuntimeException.FailureType.TIMEOUT)) {
                cacheClient.set(key, context.getConfig().getFailedQueryTTLInSec(), (T) failedPlaceHolderResult);
            }

            throw ex;
        }
    }

    public static String generateKey(Object nativeQuery, DSQuery query, ExecutionContext context) {
        Iterable<SelectColumn> derivedColumns = query.getDerivedColumns();
        final StringBuilder cacheKey = new StringBuilder();
        cacheKey.append(nativeQuery);
        for (SelectColumn derivedColumn : derivedColumns) {
            cacheKey.append("|||")
                    .append(((SelectColumn.Expression) derivedColumn).expressionString);
        }

        Map<String, String[]> param = context.getParams();
        if (query.getDateHistogramCol().isPresent() && !query.hasGroupBys()) {
            SelectColumn.DateHistogram dateHistogram = query.getDateHistogramCol().get();
            if (dateHistogram.getSeriesType(param).equals(SelectColumn.SeriesType.CUMULATIVE)) {
                cacheKey.append(String.valueOf(SelectColumn.SeriesType.CUMULATIVE));
            } else if (dateHistogram.getSeriesType(param).equals(SelectColumn.SeriesType.GROWTH)) {
                cacheKey.append(String.valueOf(SelectColumn.SeriesType.GROWTH));
            }
        }

        return cacheKey.toString();
    }

    private boolean checkStaleness(QueryResult queryResult, long factCreatedTime) {
        long factCreatedAtTimeFromCache = queryResult.getFactCreatedAtTime();
        if (factCreatedAtTimeFromCache != factCreatedTime)
        {
            cacheLogger.info(String.format("staleness is present, query.factCreatedTime= %d, cosmos.factCreatedTime=%d ",
                    factCreatedAtTimeFromCache, factCreatedTime));
            return true;
        }
        return false;
    }

    private Optional<T> checkPlaceholderAndGet(ICacheClient<String, T> cacheClient,
                                               String cacheKey, int checkPlaceholderPollingTimeMs) {
        Optional<T> queryResultOptional = cacheClient.get(cacheKey);

        while (queryResultOptional.isPresent() &&
                CACHE_INTERNAL_PLACEHOLDER.equals(queryResultOptional.get().getFactName())) {
            try {
                Thread.sleep(checkPlaceholderPollingTimeMs);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            queryResultOptional = cacheClient.get(cacheKey);
        }
        return queryResultOptional;
    }

    private void checkIfCacheHasErrorsAndThrow(ICacheClient<String, T> cacheClient, String cacheKey)
    {

        Optional<T> queryResultOptional = cacheClient.get(cacheKey);

        if (queryResultOptional.isPresent() &&
                queryResultOptional.get().getHasFailed()) {

            String errorMessage = queryResultOptional.get().getErrorMessage();

            if (errorMessage != null && (errorMessage.contains("Execution time exceeded run time cap")
                    || errorMessage.contains("Join inner did not fit in memory")
                    || errorMessage.contains("inner partition did not fit in memory")
                    || errorMessage.contains("Insufficient resources to execute plan on pool reporting"))) {
                String exceptionMessage = String.format("The query had failed recently due to resource shortage at vertica.\n" +
                        "Please try again after sometime.");
                throw new QueryResourceConstraintException(exceptionMessage);
            }
            else if(queryResultOptional.get().errorType != null &&  queryResultOptional.get().errorType  == HystrixRuntimeException.FailureType.TIMEOUT)
            {
                throw new QueryTimeoutException(errorMessage);
            }
            throw new CachedException(errorMessage, queryResultOptional.get().exceptionStack, cacheKey);
        }


    }

    @LogExecTime
    private Optional<T> getFromCache(ICacheClient<String, T> cacheClient,
                                     String cacheKey, long factCreatedTime, int checkPlaceholderPollingTimeMs) {

        Optional<T> queryResultOptional = checkPlaceholderAndGet(cacheClient, cacheKey, checkPlaceholderPollingTimeMs);

        final String msg = "[%s] key(hash code)='%s', query='%s', factRefreshTime='%s'";
        String type = "Cache miss";

        if (queryResultOptional.isPresent()) {
            final QueryResult queryResult = queryResultOptional.get();

            if (queryResult.getHasFailed())
                return queryResultOptional;

            if (checkStaleness(queryResult, factCreatedTime)) {
                cacheClient.remove(cacheKey);
                queryResultOptional = Optional.empty();
                type = "Cache evict";
            } else {
                type = "Cache hit";
            }
        }
        cacheLogger.info(String.format(msg, type, cacheKey.hashCode(), cacheKey, String.valueOf(factCreatedTime)));

        return queryResultOptional;
    }

    private long getCacheKeyTTLInSec(ExecutionContext context){

//        Map<String,String> factProperties = MetaAccessor.get().getTablePropertiesByTableName(context.getFromTable());
//        String ttlKey = TablePropertiesDao.PropertyKeys.TTL.toString();
//        try {
//            if (factProperties.containsKey(ttlKey) && StringUtils.isNotBlank(factProperties.get(ttlKey)))
//                cacheKeyTTLInSec = Long.parseLong(factProperties.get(ttlKey));
//        }catch (Exception e){
//            cacheLogger.error("Exception while retrieving fact level TTL",e);
//        }
        return cacheKeyTTLInSec;
    }
}
