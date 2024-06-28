package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs changes to state by {@link AsyncTask}
 */
public class LoggerListner extends Observer {
    public static final Logger logger = LoggerFactory.getLogger("ASYNC_POLICY_LOG");

    QueryResultMeta.QueryStatus previousState = QueryResultMeta.QueryStatus.INVALID;
    QueryHandle handle;

    public LoggerListner(AsyncTask<Void> subject, QueryHandle handle) {
        super(subject);
        this.handle = handle;
    }

    @Override
    public void notifyStateChange(QueryResultMeta.QueryStatus state) {
        logger.debug("State changed for handle {} changed from {} to {}", new Object[] {
                handle.getHandle(), previousState, state
        });
        previousState = state;
    }

    @Override
    public void notifyResults(Optional<QueryResult> results) {
        logger.debug("Notified results {} for handle {} found and the state is {}", new Object[] {
                results.isPresent(), handle.getHandle(), previousState
        });
    }

    @Override
    public void notifyFailure(Exception ex) {
        logger.debug("Notified failure for handle {}, and the state is {}", new Object[] {
                handle.getHandle(), previousState, ex});
    }
}
