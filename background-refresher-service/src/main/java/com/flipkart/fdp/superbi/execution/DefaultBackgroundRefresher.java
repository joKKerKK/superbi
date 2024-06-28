package com.flipkart.fdp.superbi.execution;

import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.BackgroundRefresher;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@Slf4j
public class DefaultBackgroundRefresher implements BackgroundRefresher {

    private final Function<String, BackgroundRefresherConfig> backgroundRefresherConfigProvider;
    private final BackgroundRefreshTaskExecutor taskExecutor;
    private final RetryTaskHandler retryTaskHandler;
    private final LockDao lockDao;

    public DefaultBackgroundRefresher(
            LockDao lockDao,
            Function<String, BackgroundRefresherConfig> backgroundRefresherConfigProvider,
            BackgroundRefreshTaskExecutor backgroundRefreshTaskExecutor,
            RetryTaskHandler retryTaskHandler) {
        this.lockDao = lockDao;
        this.backgroundRefresherConfigProvider = backgroundRefresherConfigProvider;
        this.taskExecutor = backgroundRefreshTaskExecutor;
        this.retryTaskHandler = retryTaskHandler;
    }

    private BackgroundRefreshTask createTask(QueryPayload queryPayload) {
        String storeIdentifier = queryPayload.getStoreIdentifier();
        BackgroundRefresherConfig backgroundRefresherConfig = backgroundRefresherConfigProvider
                .apply(storeIdentifier);
        return BackgroundRefreshTask.builder()
                .queryPayload(queryPayload)
                .remainingRetry(backgroundRefresherConfig.getNumOfRetryOnException())
                .executionAfterTimestamp(System.currentTimeMillis() + backgroundRefresherConfig
                        .getRetryOnExceptionBackoffInMillis())
                .lockDao(lockDao)
                .retryTaskHandler(this.retryTaskHandler)
                .backgroundRefreshTaskExecutor(taskExecutor)
                .backgroundRefresherConfig(backgroundRefresherConfig).build();
    }


    @Override
    public void submitQuery(QueryPayload queryPayload) {
        log.info("DefaultBackgroundRefresher - submitQuery called");
        try {
            log.info("submitting query",queryPayload);
            createTask(queryPayload).executeAsync();
        } catch (Exception ignore) {
            log.error("Could not submitQuery. Ignoring", ignore);
        }
    }
}
