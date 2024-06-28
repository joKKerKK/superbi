package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.dsl.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by arun.khetarpal on 17/09/15.
 */
public class NativeQueryTranslator {
    private static final Logger logger = LoggerFactory.getLogger(NativeQueryTranslator.class);
    private Object nativeQuery;
    private long elapsedTimeMs;

    public NativeQueryTranslator(ExecutionContext context) {
        Timer translationTimer = new Timer().start();
        nativeQuery = context.getExecutor().getTranslator(context.getQuery(), context.getParams()).buildQuery();
        translationTimer.stop();
        elapsedTimeMs = translationTimer.getTimeTakenMs();

        logger.info("Native Query: {}", nativeQuery);
    }

    public Object getTranslatedQuery() {
        return nativeQuery;
    }

    public long getElapsedTimeMs() {
        return elapsedTimeMs;
    }
}
