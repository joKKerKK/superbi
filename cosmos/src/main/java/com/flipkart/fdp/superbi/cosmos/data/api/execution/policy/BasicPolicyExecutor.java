package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ServerSideTransformer;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import java.util.Map;
import java.util.Optional;


/**
 * Created by arun.khetarpal on 17/09/15.
 */
public class BasicPolicyExecutor<T extends QueryResult> implements Executable<T> {
    protected long elapsedTime;

    @Override
    public long elapsedTimeMs() {
        return elapsedTime;
    }

    @Override
    @LogExecTime
    public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator) {

        final Timer elapsedTimer = new Timer().start();

        String threadPool = context.getSourceName();
        String federation = context.getFederationType().toString();
        DSQueryExecutor executor = context.getExecutor();
        Object nativeQuery = translator.getTranslatedQuery();
        DSQuery dsQuery = context.getQuery();
        Map<String, String[]> params = context.getParams();

        int timeoutMs = (int) context.getConfig().getQueryTimeOutMs();
        Optional<T> queryResultOptional;

        try {
            final QueryResult rawQueryResult = new RemoteCall.Builder<QueryResult>(threadPool, federation)
                    .withTimeOut(timeoutMs).around(new ActualCall<QueryResult>() {
                        public QueryResult workUnit() {
                            return executor.executeNative(nativeQuery,
                                    new DSQueryExecutor.ExecutionContext(dsQuery, params));
                        }
                    }).execute();
            queryResultOptional = Optional.ofNullable((T)
                    ServerSideTransformer.getFor(dsQuery, params, null).postProcess(rawQueryResult));
            return queryResultOptional;

        } catch (Exception ex) {
            throw ex;
        } finally {
            elapsedTimer.stop();
            elapsedTime = elapsedTimer.getTimeTakenMs();
        }
    }
}
