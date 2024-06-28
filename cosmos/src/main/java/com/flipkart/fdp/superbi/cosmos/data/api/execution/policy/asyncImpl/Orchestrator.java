package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.NativeQueryTranslator;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import java.util.concurrent.Future;

/**
 * Top level class which manages and oversees the entire stages for async execution
 */
public class Orchestrator {

    private ExecutionContext context;
    private NativeQueryTranslator translator;
    private QueryHandle queryHandle;
    private long factCreatedAtTime;
    private ICacheClient<String, QueryResultMeta> queryResultMetaStore;
    private ICacheClient<String, QueryResult> queryResultStore;

    private static int TTL_GRACE_IN_SEC = 5;

    public Orchestrator(ExecutionContext context, NativeQueryTranslator translator, QueryHandle queryHandle,
                        long factCreatedAtTime, ICacheClient<String, QueryResultMeta> queryResultMetaStore,
                        ICacheClient<String, QueryResult> queryResultStore) {
        this.context = context;
        this.translator = translator;
        this.queryHandle = queryHandle;
        this.factCreatedAtTime = factCreatedAtTime;
        this.queryResultMetaStore = queryResultMetaStore;
        this.queryResultStore = queryResultStore;
    }

    public void orchestrate() {
        String threadPool = context.getSourceName();
        AsyncTask<Void> task = new AsyncTask<>(context, translator, queryHandle, factCreatedAtTime, hystrixTimeOutInSec(), generateResultExpireTime());
        HandleValidationListner hvl = new HandleValidationListner(task);

        task.attach(new StateListner(task, queryResultMetaStore));
        task.attach(new ResultListner(task, queryResultStore));
        task.attach(new LoggerListner(task, queryHandle));
        task.attach(new QueryInfoLogListner(task));
        task.attach(hvl);

        Future<Void> hystrixHandle = new RemoteCall.Builder<Void>(threadPool)
                .withTimeOut(hystrixTimeOutInSec() * 1000)
                .around(task).executeAsync();

        hvl.validateHandle();
    }

    private int hystrixTimeOutInSec() {
        return (getPostProcessingTimeOut() + getQueryTimeOut() + TTL_GRACE_IN_SEC);
    }

    private int getQueryTimeOut() {
        return (int) context.getConfig().getQueryTimeOutMs() / 1000;
    }

    private int getPostProcessingTimeOut() {
        return context.getConfig().getPostProcessingTTLInSec();
    }

    private long generateResultExpireTime() { return (System.currentTimeMillis() / 1000L) + hystrixTimeOutInSec() + context.getConfig().getDefaultTTLInSec(); }
}
