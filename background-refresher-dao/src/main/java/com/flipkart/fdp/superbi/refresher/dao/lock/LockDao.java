package com.flipkart.fdp.superbi.refresher.dao.lock;

import java.io.Serializable;

public interface LockDao extends Serializable {
    void acquireLock(String lockKey, long ttl);
    void releaseLock(String lockKey);
    boolean isLock(String lockKey);
}
