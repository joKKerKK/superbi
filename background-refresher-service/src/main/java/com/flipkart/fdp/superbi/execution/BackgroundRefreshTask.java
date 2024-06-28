package com.flipkart.fdp.superbi.execution;

import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;

@Builder
@Getter
@Slf4j
@ToString
public class BackgroundRefreshTask {

  private static final int MIN_BACKOFF_JITTER_PERCENTAGE = 10;
  private static final int MAX_BACKOFF_JITTER_PERCENTAGE = 20;

  private final QueryPayload queryPayload;
  private final int remainingRetry;
  private final long executionAfterTimestamp;
  private final LockDao lockDao;
  private final BackgroundRefresherConfig backgroundRefresherConfig;
  private final RetryTaskHandler retryTaskHandler;

  @Getter(AccessLevel.NONE)
  private final BackgroundRefreshTaskExecutor backgroundRefreshTaskExecutor;

  void executeAsync() {
    log.info("BackgroundRefreshTask - executeAsync called");
    backgroundRefreshTaskExecutor.executeTaskAsync(this).subscribe();
  }

  private BackgroundRefreshTask copy(int remainingRetry, long executionAfterTimestamp) {
    return BackgroundRefreshTask.builder()
            .lockDao(this.lockDao)
        .queryPayload(this.getQueryPayload())
        .remainingRetry(remainingRetry)
        .executionAfterTimestamp(executionAfterTimestamp)
        .backgroundRefresherConfig(this.backgroundRefresherConfig)
        .retryTaskHandler(this.retryTaskHandler)
        .backgroundRefreshTaskExecutor(this.backgroundRefreshTaskExecutor).build();
  }

  public void submitForRetry() {
    BackgroundRefreshTask retryTask = this
        .copy(this.remainingRetry - 1,
            calculateExecutionAfterTimestamp(
                this.backgroundRefresherConfig.getRetryOnExceptionBackoffInMillis()));
    //Take lock for BackoffTime.
    int lockTTL = (int) (retryTask.backgroundRefresherConfig.getRetryOnExceptionBackoffInMillis()
        / 1000);
    lockDao.acquireLock(getLockKey(),lockTTL);
    this.retryTaskHandler.submitForRetry(retryTask);
  }


  private long calculateExecutionAfterTimestamp(long backoffInMillis) {
    long jitterInMillis = ThreadLocalRandom.current()
        .nextLong(Math
            .round(backoffInMillis * (float) MIN_BACKOFF_JITTER_PERCENTAGE / 100), Math
            .round(backoffInMillis * (float) MAX_BACKOFF_JITTER_PERCENTAGE / 100));

    //Retry timestamp is current time + backoff + random jitter.
    return System.currentTimeMillis() + backoffInMillis + jitterInMillis;
  }

  String getCacheKey() {
    return this.queryPayload.getCacheKey();
  }

  public void unLockTask(){
    lockDao.releaseLock(getLockKey());
  }

  public void acquireLock(int ttl){
    lockDao.acquireLock(getLockKey(),ttl);
  }

  public boolean isLock(){
    return lockDao.isLock(getLockKey());
  }

  public final int getAttemptNumber() {
    return 1 + (getBackgroundRefresherConfig().getNumOfRetryOnException() - getRemainingRetry());
  }

  private String getLockKey() {
    return "lock_key_" + this.getCacheKey();
  }
}