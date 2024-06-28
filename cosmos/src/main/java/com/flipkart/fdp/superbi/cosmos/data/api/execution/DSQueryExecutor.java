package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 19/08/14
 */

public abstract class DSQueryExecutor {

    public static class ExecutionContext {

        public final DSQuery query;
        public final Map<String, String[]> params;
        public ExecutionContext(DSQuery query, Map<String, String[]> params) {
            this.query = query;
            this.params = params;
        }

    }
    /** DI ed at executor level as source type level overrides might not be sufficient -
     * different tweaks might be needed for sources of same type (viz ES nm and ch)
     * **/
    protected final AbstractDSLConfig config;
    protected final String sourceName;
    protected final String sourceType;

    private DSQueryExecutor() {config = null; sourceName = null; sourceType = null;}

    public DSQueryExecutor (String sourceName, String sourceType, AbstractDSLConfig config) {
        this.config = config;
        this.sourceName = sourceName;
        this.sourceType = sourceType;
    }

    public abstract AbstractQueryBuilder getTranslator(DSQuery query, Map<String, String[]> paramValues);
    public abstract Object explainNative(Object nativeQuery);
    public abstract QueryResult executeNative(Object object, ExecutionContext context);
    public abstract QueryResult executeNative(Object object, ExecutionContext context,ICacheClient<String,QueryResult> cacheClient);

    public abstract StreamingQueryResult executeStreamNative(Object object, ExecutionContext context);

    public final AbstractDSLConfig getDSLConfig() {return config;}

    public final String getSourceName() {
        return sourceName;
    }

    public final String getSourceType() {
        return sourceType;
    }
}
