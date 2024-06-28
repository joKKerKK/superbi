package com.flipkart.fdp.superbi.subscription.audit;

import com.flipkart.fdp.superbi.subscription.configurations.SubscriptionConfig;
import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompositeAuditor implements Auditor{

  private final List<Auditor> auditors;
  private final SubscriptionConfig subscriptionConfig;

  @Inject
  public CompositeAuditor(
      @Named("AUDITORS") List<Auditor> auditors, SubscriptionConfig subscriptionConfig) {
    this.auditors = auditors;
    this.subscriptionConfig = subscriptionConfig;
  }

  @Override
  public boolean isAuditorEnabled() {
    return subscriptionConfig.isAuditorEnabled();
  }

  @Override
  public void audit(ScheduleInfoLog scheduleInfoLog) {
    if (isAuditorEnabled()) {
      auditors.forEach(auditer -> {
        try {
          auditer.audit(scheduleInfoLog);
        }catch (Exception e) {
          log.error("Auditer error : ", e);
        }
      });
    }
  }
}
