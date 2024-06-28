package com.flipkart.fdp.superbi.brv2.execution;

import com.flipkart.fdp.superbi.execution.BackgroundRefreshTask;
import com.flipkart.fdp.superbi.execution.RetryTaskHandler;

public class DummyRetryTaskHandler implements RetryTaskHandler {

  @Override
  public void submitForRetry(BackgroundRefreshTask task) {
    //Do nothing in case of BRv2. Since retries will be handled by Compito.
  }
}
