package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.NativeQueryTranslator;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;

/**
 * Audits using {@link ExecutorQueryInfoLog} info audit database
 * This class fulfills auditing as a part of DG initiative
 */
public class QueryInfoLogListner extends Observer {

    ExecutorQueryInfoLog.Builder executorQueryInfoLogBuilder = new ExecutorQueryInfoLog.Builder();
    Timer timer = new Timer();

    public QueryInfoLogListner(AsyncTask<Void> subject) {
        super(subject);

        timer.start();
        ExecutionContext context = subject.getContext();
        NativeQueryTranslator translator = subject.getTranslator();

        executorQueryInfoLogBuilder
                .setDsQuery("tbd")
                .setCacheHit(false)
                .setSourceName(context.getSourceName())
                .setSourceType(context.getSourceType())
                .setTranslatedQuery(translator.getTranslatedQuery().toString())
                .setStartTimeStampMs(timer.getStartTimeMs())
                .setTranslationTimeMs(translator.getElapsedTimeMs());
    }

    @Override
    public void notifyStateChange(QueryResultMeta.QueryStatus state) {
        switch (state) {
            case INVALID:
                break;
            case SUBMITTED:
                break;
            case RUNNING:
                break;
            case POST_PROCESSING:
                executorQueryInfoLogBuilder.setExecutionTimeMs(timer.getElapsedTimeTakenMs());
                break;
            case SUCCESSFUL: {
                executorQueryInfoLogBuilder.setIsCompleted(true);
                commitResult();
                break;
            }
            case FAILED: {
                executorQueryInfoLogBuilder.setIsCompleted(false);
                commitResult();
            }
        }
    }

    private void commitResult() {
        timer.stop();
        executorQueryInfoLogBuilder.setTotalTimeMs(timer.getTimeTakenMs())
                                   .setIsSlowQuery(
                                           timer.getTimeTakenMs() > subject.getContext().getConfig().getSlowQueryTimeOutMs());
        MetaCreator.get().logExecutorQueryInfo(executorQueryInfoLogBuilder.build());
    }
}
