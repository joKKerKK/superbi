package com.flipkart.fdp.superbi.subscription.executors;

import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;

public interface RetryJobHandler {
  void submitForRetry(ScheduleInfo scheduleInfo);
}
