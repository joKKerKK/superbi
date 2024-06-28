package com.flipkart.fdp.superbi.subscription.audit;

import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog;

public interface Auditor {

  boolean isAuditorEnabled();

  void audit(ScheduleInfoLog scheduleInfoLog);
}
