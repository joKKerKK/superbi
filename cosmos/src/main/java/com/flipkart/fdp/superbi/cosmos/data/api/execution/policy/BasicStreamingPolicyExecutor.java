package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ServerSideTransformer;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import java.util.Map;
import java.util.Optional;


/**
 * Created by arun.khetarpal on 17/09/15.
 */
public class BasicStreamingPolicyExecutor<T extends StreamingQueryResult> implements Executable {
    protected long elapsedTime;

    @Override
    public long elapsedTimeMs() {
        return elapsedTime;
    }

    @Override
    @LogExecTime
    public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator) {

        final Timer elapsedTimer = new Timer().start();

        DSQueryExecutor executor = context.getExecutor();
        Object nativeQuery = translator.getTranslatedQuery();
        DSQuery dsQuery = context.getQuery();
        Map<String, String[]> params = context.getParams();

        try {
            final StreamingQueryResult queryResult = executor.executeStreamNative(nativeQuery,
                    new DSQueryExecutor.ExecutionContext(dsQuery, params));
            return Optional.ofNullable((T)
                    ServerSideTransformer.getFor(dsQuery, params, null).postProcess(queryResult));
        } catch (Exception ex) {
            throw ex;
        } finally {
            elapsedTimer.stop();
            elapsedTime = elapsedTimer.getTimeTakenMs();
        }
    }
}
