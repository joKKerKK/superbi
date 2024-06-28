package com.flipkart.fdp.superbi.refresher.dao.lock.impl;

import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.result.cache.LockedForExecution;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.exceptions.CanNotAcquireLockException;
import com.flipkart.fdp.superbi.refresher.dao.exceptions.CanNotCheckLockException;
import com.flipkart.fdp.superbi.refresher.dao.exceptions.CanNotReleaseLockException;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;

@Slf4j
public class DefaultLockDaoImpl implements LockDao {

    private final CacheDao cacheDao;

    public DefaultLockDaoImpl(CacheDao cacheDao) {
        this.cacheDao = cacheDao;
    }

    @Override
    public void acquireLock(String lockKey, long ttl) {
        try {
            LockedForExecution lockValue = LockedForExecution.builder()
                    .resultKey(lockKey)
                    .cachedAtTime(new Date().getTime())
                    .build();

            cacheDao.set(lockKey, (int) ttl, lockValue);
        } catch (Exception ex) {
            log.error("Error acquiring lock for key - <{}> with exception -> {} ",lockKey, ex.getMessage());
            throw new CanNotAcquireLockException(ex);
        }
    }

    @Override
    public void releaseLock(String lockKey) {
        try {
            cacheDao.remove(lockKey);
        } catch (Exception ex) {
            log.error("Error releasing lock for key - <{}> with exception -> {}",lockKey, ex.getMessage());
            throw new CanNotReleaseLockException(ex);
        }
    }

    @Override
    public boolean isLock(String lockKey) {
        try {
            return cacheDao.get(lockKey,LockedForExecution.class).isPresent();
        } catch (Exception e) {
            log.error("Cannot check lock for key - <{}> with exception -> {} ",lockKey, e.getMessage());
            throw new CanNotCheckLockException(e);
        }
    }
}
