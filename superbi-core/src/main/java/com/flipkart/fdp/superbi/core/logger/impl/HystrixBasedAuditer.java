package com.flipkart.fdp.superbi.core.logger.impl;

import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.model.AuditInfo;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.ServiceConfigDefaults;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 29/08/19
 */

@Getter
public abstract class HystrixBasedAuditer implements Auditer {
  protected final String auditerName;
  protected final int maxThreads;
  protected final int requestTimeoutInMillies;


  public HystrixBasedAuditer(String auditerName, int maxThreads, int requestTimeoutInMillies) {
    this.auditerName = auditerName;
    this.maxThreads = maxThreads;
    this.requestTimeoutInMillies = requestTimeoutInMillies;
    ServiceConfigDefaults.inject(getHystrixConfig());
  }

  protected ServiceConfigDefaults.Config getHystrixConfig() {
    ServiceConfigDefaults.Config.Builder configBuilder = new ServiceConfigDefaults.Config.Builder();
    ServiceConfigDefaults.Config hystrixConfig = configBuilder
        .withMaxThreads(maxThreads)
        .forServiceGroup(auditerName);

    return hystrixConfig;
  }

  @Override
  public void audit(AuditInfo auditInfo) {
    if (isAuditorEnabled()) {
      new RemoteCall.Builder<Boolean>(auditerName)
          .withTimeOut(requestTimeoutInMillies)
          .around(new ActualCall<Boolean>() {
            @Override
            public Boolean workUnit() {
              return _audit(auditInfo);
            }
          }).executeAsync();
    }
  }

  @Override
  public void audit(ExecutorQueryInfoLog log) {
    if (isAuditorEnabled()) {
      new RemoteCall.Builder<Boolean>(auditerName)
          .withTimeOut(requestTimeoutInMillies)
          .around(new ActualCall<Boolean>() {
            @Override
            public Boolean workUnit() {
              return _audit(log);
            }
          }).executeAsync();
    }
  }

  @Override
  public void audit(final QueryInfo queryInfo) {
    if (isAuditorEnabled()) {
      new RemoteCall.Builder<Boolean>(auditerName)
          .withTimeOut(requestTimeoutInMillies)
          .around(new ActualCall<Boolean>() {
            @Override
            public Boolean workUnit() {
              return _audit(queryInfo);
            }
          }).executeAsync();
    }
  }

  protected abstract boolean _audit(AuditInfo auditInfo);

  protected abstract boolean _audit(ExecutorQueryInfoLog log);

  protected abstract boolean _audit(final QueryInfo queryInfo);
}
