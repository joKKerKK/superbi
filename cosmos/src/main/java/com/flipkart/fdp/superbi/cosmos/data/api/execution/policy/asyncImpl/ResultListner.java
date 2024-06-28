package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import java.util.Date;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages all the results while executing the {@link AsyncTask}
 */
public class ResultListner extends Observer {
    private ICacheClient<String, QueryResult> queryResultStore;
    private QueryResultMeta.QueryStatus state;
    private static int GRACE_TIME_IN_SEC = 2;
    public static final Logger logger = LoggerFactory.getLogger("ASYNC_POLICY_LOG");

    public ResultListner(AsyncTask<Void> subject, ICacheClient<String, QueryResult> queryResultStore) {
        super(subject);
        this.queryResultStore = queryResultStore;
    }

    @Override
    public void notifyResults(Optional<QueryResult> results) {
        assert results.isPresent();

        QueryHandle handle = subject.getHandle();
        boolean flag = queryResultStore.set(handle.getHandle(), (int) getResultStoreAbsTTL(), results.get());

        if (flag) {
            logger.debug("Successfully written results to Cache for handle: {}", new Object[] {
                    handle.getHandle()
            });
        } else {
            logger.debug("Failed to write results to Cache for handle: {}", new Object[] {
                    handle.getHandle()
            });
        }
        logger.debug("Result for handle {} expires at {}", new Object[]{
                handle.getHandle(), new Date(getResultStoreAbsTTL())});
    }

    private long getResultStoreAbsTTL() { return subject.getResultExpireTime() + GRACE_TIME_IN_SEC; }
}
