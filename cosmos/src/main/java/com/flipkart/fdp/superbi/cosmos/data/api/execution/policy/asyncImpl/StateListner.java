package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages all the state changes which happen while executing the {@link AsyncTask}
 */
public class StateListner extends Observer {

    public static final Logger logger = LoggerFactory.getLogger(StateListner.class);
    private ICacheClient<String, QueryResultMeta>  queryResultMetaStore;
    private boolean queryFailed = false;

    public StateListner(AsyncTask<Void> subject,
                        ICacheClient<String, QueryResultMeta> queryResultMetaStore) {
        super(subject);
        this.queryResultMetaStore = queryResultMetaStore;
    }

    /**
     * House all the changes in the meta store.
     * Todo: validate state changes
     * @param state
     */
    @Override
    public void notifyStateChange(QueryResultMeta.QueryStatus state) {
        QueryHandle handle = subject.getHandle();
        Optional<QueryResultMeta> queryResultMeta = queryResultMetaStore.get(handle.getHandle());

        switch(state) {
            case SUBMITTED:
            {
                QueryResultMeta meta = new QueryResultMeta.Builder()
                        .setStatus(QueryResultMeta.QueryStatus.SUBMITTED)
                        .setSubmittedAtTime(new Date().getTime())
                        .setFactCreatedAtTime(subject.getFactCreatedAtTime())
                        .setHostName(getHostName()).setThreadId(Thread.currentThread().getName())
                        .setParams(subject.getContext().getParams())
                        .setCached(queryResultMeta.isPresent() ? queryResultMeta.get().getCached() : false)
                        .build();
                setMeta(handle, getQueryTimeOut(), meta, state);
                //queryResultMetaStore.set(handle.getHandle(), getQueryTimeOut(), meta /*placeholder*/);
                break;
            }

            case RUNNING:
            {
                if (!queryResultMeta.isPresent()) throw new InvalidStateTransition("No previous state found in meta");

                QueryResultMeta meta = new QueryResultMeta.Builder(queryResultMeta.get())
                        .setStatus(QueryResultMeta.QueryStatus.RUNNING)
                        .setExecutionStartTime(new Date().getTime())
                        .setHostName(getHostName())
                        .setThreadId(Thread.currentThread().getName())
                        .build();
                setMeta(handle, getQueryTimeOut(), meta, state);
                //queryResultMetaStore.set(handle.getHandle(), getQueryTimeOut(), meta);
                break;
            }

            case POST_PROCESSING:
            {
                if (!queryResultMeta.isPresent()) throw new InvalidStateTransition("No previous state found in meta");

                QueryResultMeta meta = new QueryResultMeta.Builder(queryResultMeta.get())
                        .setStatus(QueryResultMeta.QueryStatus.POST_PROCESSING)
                        .setExecutionEndTime(new Date().getTime())
                        .setProcessingStartTime(new Date().getTime())
                        .build();
                setMeta(handle, getPostProcessingTimeOut(), meta, state);
                //queryResultMetaStore.set(handle.getHandle(), getPostProcessingTimeOut(), meta);
                break;
            }

            case SUCCESSFUL:
            {
                if (!queryResultMeta.isPresent()) throw new InvalidStateTransition("No previous state found in meta");

                QueryResultMeta meta = new QueryResultMeta.Builder(queryResultMeta.get())
                        .setStatus(QueryResultMeta.QueryStatus.SUCCESSFUL)
                        .setProcessingEndTime(new Date().getTime())
                        .setCached(true)
                        .setCachedAtTime(new Date().getTime())
                        .build();
                setMeta(handle, getMetaStoreAbsTTL(), meta, state);
                //queryResultMetaStore.set(handle.getHandle(), getMetaStoreAbsTTL(), meta);
                break;
            }

            case FAILED:
            {
                if (!queryResultMeta.isPresent()) throw new InvalidStateTransition("No previous state found in meta");
                this.queryFailed = true;
                break;
            }
        }
    }

    public void notifyFailure(Exception ex) {
        assert queryFailed = true;

        QueryHandle handle = subject.getHandle();

        Optional<QueryResultMeta> queryResultMeta = queryResultMetaStore.get(handle.getHandle());
        if (!queryResultMeta.isPresent()) throw new InvalidStateTransition("No previous state found in meta");

        QueryResultMeta meta = new QueryResultMeta.Builder(queryResultMeta.get())
                .setStatus(QueryResultMeta.QueryStatus.FAILED)
                .setErroredAtTime(new Date().getTime())
                .setErrorMessage(ex.getMessage())
                .build();
        setMeta(handle, getMetaStoreAbsTTL(), meta, QueryResultMeta.QueryStatus.FAILED);
        //queryResultMetaStore.set(handle.getHandle(), getMetaStoreAbsTTL(), meta);
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().toString();
        } catch(UnknownHostException uhe) {
            logger.error("InetAddress could not determine local hostname {}", uhe);
            return "Unknown";
        }
    }

    private int getQueryTimeOut() { return (int) subject.getContext().getConfig().getQueryTimeOutMs() / 1000; }

    private int getPostProcessingTimeOut() { return subject.getContext().getConfig().getPostProcessingTTLInSec(); }

    private int getMetaStoreAbsTTL() { return (int) subject.getResultExpireTime(); }

    private void setMeta(QueryHandle handle, int timeout, QueryResultMeta meta, QueryResultMeta.QueryStatus state) {
        boolean flag = queryResultMetaStore.set(handle.getHandle(), timeout, meta);

        logger.debug("Meta for handle {} with state {} expires at {}", new Object[] {
                handle.getHandle(), meta.getStatus(), new Date(timeout)});

        if (flag) {
            logger.debug("Successfully written meta to Cache for handle: {} to {} state", new Object[] {
                    handle.getHandle(), state.toString()
            });
        } else {
            logger.debug("Failed to write meta to Cache for handle: {} to {} state", new Object[] {
                    handle.getHandle(), state.toString()
            });
        }
    }
}

class InvalidStateTransition extends RuntimeException {
    public InvalidStateTransition() {}
    public InvalidStateTransition(String message) {
        super(message);
    }
    public InvalidStateTransition(String message, Throwable cause) {
        super(message, cause);
    }
    public InvalidStateTransition(Throwable cause) {
        super(cause);
    }
}
