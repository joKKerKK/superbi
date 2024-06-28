package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.ExecutorStore.EXECUTOR_STORE;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.ExecutionEventObserver;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ExecutorFacade;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Source;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by arun.khetarpal on 17/09/15.
 */
public class ExecutionContext {
    protected static final Logger logger = LoggerFactory.getLogger(ExecutionContext.class);
    protected AbstractDSLConfig config;
    protected DSQuery query;
    protected Map<String, String[]> params;
    protected DSQueryExecutor executor;
    protected String sourceName;
    protected Source.FederationType federationType = Source.FederationType.DEFAULT;
    protected SourceType sourceType;
    protected String fromTable;
    protected Optional<ICacheClient<String, QueryResult>> queryResultStoreOptional;
    protected Optional<ICacheClient<String, QueryResultMeta>> queryResultMetaStoreOptional;
    protected Optional<ICacheClient<String,String>> distributedLockStoreOptional;
    protected ExecutionEventObserver executionEventObserver;
    protected int attemptNumber = 1;
    protected String requestId;

    protected ExecutionContext() {
        queryResultStoreOptional = Optional.empty();
        queryResultMetaStoreOptional = Optional.empty();
        distributedLockStoreOptional = Optional.empty();
    }

    protected ExecutionContext(Builder<?> builder) {
        ExecutionContext that = builder.instance;
        this.query = that.query;
        this.params = that.params;
        this.queryResultStoreOptional = that.queryResultStoreOptional;
        this.queryResultMetaStoreOptional = that.queryResultMetaStoreOptional;
        this.distributedLockStoreOptional = that.distributedLockStoreOptional;
        this.executionEventObserver = that.executionEventObserver;
        this.federationType = that.federationType;
        this.attemptNumber = that.attemptNumber;
        this.requestId = that.requestId;
        init();
    }

    protected void init() {
        assert query != null && params != null;
        initSourceName();
        executor = EXECUTOR_STORE.getFor(sourceName, federationType);
        sourceType = SourceType.valueOf(executor.getSourceType());
        config = ExecutorFacade.instance.getConfigFor(sourceName, federationType);
        fromTable = query.getFromTable();
    }


    protected void initSourceName() {
        sourceName = EXECUTOR_STORE.getSourceFor(query);
    }

    public AbstractDSLConfig getConfig() {
        return config;
    }

    public DSQuery getQuery() {
        return query;
    }

    public Map<String, String[]> getParams() {
        return params;
    }

    public DSQueryExecutor getExecutor() {
        return executor;
    }

    public String getSourceName() {
        return sourceName;
    }

    public SourceType getSourceType() {
        return sourceType;
    }

    public Source.FederationType getFederationType() {
        return federationType;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getFromTable() {
        return fromTable;
    }

    public Optional<ICacheClient<String, QueryResult>> getQueryResultStoreOptional() {
        return queryResultStoreOptional;
    }

    public Optional<ICacheClient<String, QueryResultMeta>> getQueryResultMetaStoreOptional() {
        return queryResultMetaStoreOptional;
    }

    public Optional<ICacheClient<String, String>> getDistributedLockStoreOptional() {
        return distributedLockStoreOptional;
    }

    public static class Builder<T extends Builder<T>> {
        protected final ExecutionContext instance;

        protected Builder(ExecutionContext instance) {
            this.instance = instance;
        }

        public Builder() {
            instance = new ExecutionContext();
        }

        protected T getThis() {
            return (T) this;
        }

        public T setDSQuery(DSQuery query) {
            instance.query = query;
            return getThis();
        }

        public T setParams(Map<String, String[]> params) {
            instance.params = params;
            return getThis();
        }

        public T setCacheClient(Optional<ICacheClient<String, QueryResult>> cacheClientOptional) {
            instance.queryResultStoreOptional = cacheClientOptional;
            return getThis();
        }

        public T setQueryResultMetaClient(Optional<ICacheClient<String,
                QueryResultMeta>> queryResultMetaStoreOptional) {
            instance.queryResultMetaStoreOptional = queryResultMetaStoreOptional;
            return getThis();
        }

        public T setDistributedLockStore(Optional<ICacheClient<String,String>> distributedLockStoreOptional) {
            instance.distributedLockStoreOptional = distributedLockStoreOptional;
            return getThis();
        }

        public T setExecutionEventObserver(ExecutionEventObserver executionEventObserver) {
            instance.executionEventObserver = executionEventObserver;
            return getThis();
        }

        public T setFederationType(Source.FederationType federationType) {
            if(federationType == null) {
                federationType = Source.FederationType.DEFAULT;
            }
            instance.federationType = federationType;
            return getThis();
        }

        public T setAttemptNumber(int attemptNumber) {
            if(attemptNumber > 0) {
                instance.attemptNumber = attemptNumber;
            }
            return getThis();
        }

        public T setRequestId(String requestId) {
            instance.requestId = requestId;
            return getThis();
        }

        public ExecutionContext build() {
            return new ExecutionContext(this);
        }
    }

    public void publishEvent(ExecutionEvent event, Executable executable) {
        if(executionEventObserver != null) {
            try{
                executionEventObserver.publishEvent(event, executable, this);
            }
            catch (Throwable t){
                logger.error("Error publishing event {) : {}", event.getEventType(), t.getMessage());
            }
        }
    }
}
