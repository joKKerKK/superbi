package com.flipkart.fdp.superbi.cosmos.data;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.Executable;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionEvent;

public interface ExecutionEventObserver {
  default void publishEvent(ExecutionEvent event, Executable executable, ExecutionContext
      executionContext) {
    // ignore
  }
}
