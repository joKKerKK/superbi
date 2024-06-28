package com.flipkart.fdp.superbi.cosmos.data.query.result;

import com.flipkart.fdp.superbi.cosmos.data.query.QuerySubmitResult;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ResultQueryStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * User: aniruddha.gangopadhyay
 * Date: 04/02/14
 * Time: 1:04 PM
 */
public class QueryResult extends QuerySubmitResult implements Serializable {
    /**
     * @deprecated  As of release 2.91, replaced by {@link #detailedSchema}
     */
    @Deprecated public ImmutableList<String> schema;
    public Schema detailedSchema;
    public List<ResultRow> data;
    transient private Optional<ResultQueryStats> resultQueryStatsOptional;
    public String factName;
    public long factCreatedAtTime;
    public boolean cached;
    public String errorMessage;
    public long erroredAtTime;
    public String exceptionStack;
    public boolean hasFailed;
    public String cacheKey;
    public long cachedAtTime;
    public String originalExecutorRequestId;
    public HystrixRuntimeException.FailureType errorType;

    public QueryResult(Schema schema, List<ResultRow> data,
                       Optional<ResultQueryStats> resultQueryStatsOptional) {
        this.detailedSchema = schema;
        if(schema!=null)
            this.schema = ImmutableList.copyOf(Iterables.transform(schema.columns, SelectColumn.F.name));
        this.data = data;
        this.resultQueryStatsOptional = resultQueryStatsOptional;
    }

    public QueryResult(Schema schema, List<ResultRow> data) {
        this(schema, data, Optional.<ResultQueryStats>empty());
    }

    public void setResultQueryStatsOptional(Optional<ResultQueryStats> resultQueryStatsOptional) {
        this.resultQueryStatsOptional = resultQueryStatsOptional;
    }

    public void setResultQueryStats(ResultQueryStats resultQueryStats) {
        setResultQueryStatsOptional(Optional.ofNullable(resultQueryStats));
    }

    public Optional<ResultQueryStats> getResultQueryStatsOptional() {
        if(this.resultQueryStatsOptional==null) return Optional.<ResultQueryStats>empty();
        return this.resultQueryStatsOptional;
    }

    public String getFactName() {
        return factName;
    }

    public void setFactName(String factName) {
        this.factName = factName;
    }

    public long getFactCreatedAtTime() {
        return factCreatedAtTime;
    }

    public List<ResultRow> getData() { return data; }

    public void setFactCreatedAtTime(long factCreatedAtTime) {
        this.factCreatedAtTime = factCreatedAtTime;
    }

    public boolean isCached() {
        return cached;
    }

    public void setCached(boolean cached) {
        this.cached = cached;
    }

    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public String getErrorMessage() { return errorMessage; }

    public void setErroredAtTime(long erroredAtTime) { this.erroredAtTime = erroredAtTime; }

    public long getErroredAtTime() { return erroredAtTime; }

    public void setHasFailed() { this.hasFailed = true; }

    public boolean getHasFailed() { return hasFailed; }

    public void setCacheKey(String cacheKey)
    {
        this.cacheKey = cacheKey;
    }

    public String getCacheKey()
    {
        return cacheKey;
    }

    public void setCachedAtTime(long cachedAtTime)
    {
        this.cachedAtTime = cachedAtTime;
    }

    public void setOriginalExecutorRequestId(String originalExecutorRequestId)
    {
        this.originalExecutorRequestId = originalExecutorRequestId;
    }

    public long getCachedAtTime()
    {
        return cachedAtTime;
    }

    public void setExceptionStack(String exceptionStack)
    {
        this.exceptionStack = exceptionStack;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryResult{");
        sb.append("schema=").append(schema);
        sb.append(", data=").append(data);
        getResultQueryStatsOptional().ifPresent(queryStats -> {
            sb.append(", resultStats=").append(queryStats);
        });
        sb.append('}');
        return sb.toString();
    }
}
