package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import java.util.Optional;
import rx.functions.Action1;

/**
 * Created by arun.khetarpal on 17/09/15.
 */
public enum ExecutorPolicyCreatorImpl implements ExecutorPolicyCreator {
  instance;

  public Executable<QueryResult> getBudgetedPolicy(ExecutionContext context) {
    Optional<ICacheClient<String, QueryResult>> cacheClientOptional = context
        .getQueryResultStoreOptional();
    boolean tryFromCache = cacheClientOptional.isPresent() && context.getConfig().isCacheEnabled();
    long cacheKeyTTLInSec = context.getConfig().getDefaultTTLInSec();

    if (tryFromCache) {
      return new BudgetedPolicyExecutor<QueryResult>(
          new CachePolicyExecutor<QueryResult>(cacheClientOptional.get(), cacheKeyTTLInSec,
              new BasicPolicyExecutor<QueryResult>()));
    }
    return new BudgetedPolicyExecutor<QueryResult>(new BasicPolicyExecutor<QueryResult>());
  }

  public Executable<QueryResult> getBasicPolicy(ExecutionContext context) {
    Optional<ICacheClient<String, QueryResult>> cacheClientOptional = context
        .getQueryResultStoreOptional();
    boolean tryFromCache = cacheClientOptional.isPresent() && context.getConfig().isCacheEnabled();
    long cacheKeyTTLInSec = context.getConfig().getBudgetCacheKeyTTLInSec();

    if (tryFromCache) {
      return new CachePolicyExecutor<QueryResult>(cacheClientOptional.get(), cacheKeyTTLInSec,
          new BasicPolicyExecutor<QueryResult>());
    }
    return new BasicPolicyExecutor<QueryResult>();
  }

  public Executable<StreamingQueryResult> getBudgetedStreamingPolicy(ExecutionContext context) {
    return new BudgetedPolicyExecutor<StreamingQueryResult>(new BasicStreamingPolicyExecutor
        <StreamingQueryResult>());
  }

  public Executable<QueryResult> getReactivePolicy(Action1<QueryResult> successCallback,
      Action1<Throwable> errorCallback) {
    return new BasicReactivePolicyExecutor<>(successCallback, errorCallback);
  }

  public Executable<QueryHandle> getAsyncPolicy(ExecutionContext context) {
    ICacheClient<String, QueryResult> queryResultClientStore = context.getQueryResultStoreOptional()
        .get();
    ICacheClient<String, QueryResultMeta> queryResultMetaStore = context
        .getQueryResultMetaStoreOptional().get();

    return new BasicAsyncPolicyExecutor<>(queryResultMetaStore, queryResultClientStore);
  }

  public Executable getCachePolicy(ExecutionContext context) {
    Optional<ICacheClient<String, QueryResult>> cacheClientOptional = context
        .getQueryResultStoreOptional();
    boolean tryFromCache = cacheClientOptional.isPresent() && context.getConfig().isCacheEnabled();
    long cacheKeyTTLInSec = context.getConfig().getBudgetCacheKeyTTLInSec();

    return new CachePolicyExecutor(cacheClientOptional.get(), cacheKeyTTLInSec,
        new BasicPolicyExecutor());
  }
}
