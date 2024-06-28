package com.flipkart.fdp.superbi.brv2.execution;

import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;

public class DummyLockDao implements LockDao {

  @Override
  public void acquireLock(String lockKey, long ttl) {
    //Do nothing since it will not be required by BRv2.
  }

  @Override
  public void releaseLock(String lockKey) {
    //Do nothing since it will not be required by BRv2.
  }

  @Override
  public boolean isLock(String lockKey) {
    return false;
  }
}
