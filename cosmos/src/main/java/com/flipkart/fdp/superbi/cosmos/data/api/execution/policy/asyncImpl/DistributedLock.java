package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience class for doing distributed locks.
 */
public class DistributedLock implements AutoCloseable {
    public static final Logger logger = LoggerFactory.getLogger("ASYNC_POLICY_LOG");

    public static final String LOCK_PREFIX = "LOCK:";
    public static final int MAX_SLEEP_BETWEEN_RETRIES = 1000;

    private String lockName;
    private ICacheClient<String, String> distributedLockStore;
    private boolean lockAcquired = false;
    private int acquireAttempt = 0;

    public static DistributedLock getAtomicDistributedLock(ICacheClient<String, String> distributedLockStore,
                                                           String lockName, String lockHolder,
                                                           int timeoutInSeconds) throws InterruptedException {
        DistributedLock distributedLock = new DistributedLock(lockName, distributedLockStore);
        return distributedLock;
    }

    @Override
    public void close() throws Exception {
        unlock();
    }

    public int getAcquireAttempt() {
        return acquireAttempt;
    }

    protected DistributedLock(String lockName, ICacheClient<String, String> distributedLockStore) {
        this.lockName = LOCK_PREFIX + lockName;
        this.distributedLockStore = distributedLockStore;
    }

    private boolean acquireLock(int timeoutInSeconds, String lockHolder) {
        logger.debug("Attempting to acquire lock {}, lockholder {}", lockName, lockHolder);
        lockAcquired = distributedLockStore.add(lockName, timeoutInSeconds, lockHolder);
        return lockAcquired;
    }

    public void lock(int timeoutInSeconds, String lockHolder) throws InterruptedException {
        while (true) {
            if (acquireLock(timeoutInSeconds, lockHolder)) {
                break;
            } else {
                acquireAttempt++;
                logger.debug("Lock {} not acquired. Trying again...", lockName);
                Thread.sleep(Math.min(acquireAttempt * acquireAttempt * 10, MAX_SLEEP_BETWEEN_RETRIES));
            }
        }
    }

    private boolean unlock() {
        if (lockAcquired) {
            logger.debug("Released Lock {}", lockName);
            lockAcquired = false;
            return distributedLockStore.remove(lockName);
        } else {
            return false;
        }
    }
}
