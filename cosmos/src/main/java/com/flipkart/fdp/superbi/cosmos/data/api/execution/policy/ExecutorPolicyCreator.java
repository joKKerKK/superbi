package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import rx.functions.Action1;

public interface ExecutorPolicyCreator {

  Executable<QueryResult> getBudgetedPolicy(ExecutionContext context);

  Executable<QueryResult> getBasicPolicy(ExecutionContext context);

  Executable<StreamingQueryResult> getBudgetedStreamingPolicy(ExecutionContext context);

  Executable<QueryResult> getReactivePolicy(Action1<QueryResult> successCallback,
      Action1<Throwable> errorCallback);

  Executable<QueryHandle> getAsyncPolicy(ExecutionContext context);

  Executable getCachePolicy(ExecutionContext context);
}

