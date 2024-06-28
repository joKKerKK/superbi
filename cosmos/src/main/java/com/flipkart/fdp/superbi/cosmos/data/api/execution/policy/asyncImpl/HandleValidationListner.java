package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes the main thread (user thread) wait for {@link this#MAX_THREAD_WAIT_MS}
 * or for the first state change - which ever happens before
 */
public class HandleValidationListner extends Observer {

    private final long MAX_THREAD_WAIT_MS = 500;
    private volatile boolean validHandle = false;
    private static final Logger logger = LoggerFactory.getLogger("ASYNC_POLICY_LOG");

    HandleValidationListner(AsyncTask<Void> subject) {
        super(subject);
    }

    /**
     * Notifies the user thread that handle is now valid to poll on
     * NOTE: This is a callback function which will be invoked by Hystrix Thread
     * @param state
     */
    @Override
    public void notifyStateChange(QueryResultMeta.QueryStatus state) {
        if (validHandle) return; // don't really care about more updates
        notifyStateChangeHelper(state);
    }

    public void notifyStateChangeHelper(QueryResultMeta.QueryStatus state) {
        synchronized (this) {
            validHandle = true;
            notifyAll();
        }
        logger.debug("State change {} notified by hystrix thread to user thread", state);
    }

    /**
     * Waits till the handle is valid. NOTE: Will generally be called by User thread
     */
    public void validateHandle() {
        if (!validHandle) {
            synchronized (this) {
                try {
                    wait(MAX_THREAD_WAIT_MS);
                } catch (InterruptedException e) {
                    logger.error("InterruptedException while waiting for hystrix thread {}", e);
                }
            }
        }
        logger.debug("User thread is now no longer waiting on lock with handle state {}", validHandle);
    }
}
