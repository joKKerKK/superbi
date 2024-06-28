package com.flipkart.fdp.superbi.subscription.audit;

import com.flipkart.fdp.audit.dao.subscription.ScheduleInfoAuditDao;
import com.flipkart.fdp.audit.entities.ScheduleInfoAudit;
import com.flipkart.fdp.dao.common.transaction.WorkUnit;
import com.flipkart.fdp.superbi.subscription.configurations.SubscriptionConfig;
import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog;
import com.flipkart.fdp.superbi.subscription.util.StringUtil;
import com.google.inject.Inject;

public class AuditDBLogger implements Auditor{

  private final SubscriptionConfig subscriptionConfig;
  private final ScheduleInfoAuditDao scheduleInfoAuditDao;

  @Inject
  public AuditDBLogger(
      SubscriptionConfig subscriptionConfig, ScheduleInfoAuditDao scheduleInfoAuditDao) {
    this.subscriptionConfig = subscriptionConfig;
    this.scheduleInfoAuditDao = scheduleInfoAuditDao;
  }

  @Override
  public boolean isAuditorEnabled() {
    return subscriptionConfig.isAuditorEnabled();
  }

  @Override
  public void audit(ScheduleInfoLog scheduleInfoLog) {
    ScheduleInfoAudit scheduleInfoAudit = getScheduleInfoAuditEntity(scheduleInfoLog);
    scheduleInfoAuditDao.performInTransaction(new WorkUnit() {
      @Override
      public void perform() {
        scheduleInfoAuditDao.persist(scheduleInfoAudit);
      }
    });
  }

  private ScheduleInfoAudit getScheduleInfoAuditEntity(ScheduleInfoLog scheduleInfoLog) {
    return ScheduleInfoAudit.builder().createdAt(scheduleInfoLog.getCreatedAt()).endAt(scheduleInfoLog.getEndAt())
        .message(StringUtil.shortenString(scheduleInfoLog.getMessage())).scheduleStatus(scheduleInfoLog.getScheduleStatus().getId())
        .scheduleId(scheduleInfoLog.getScheduleId()).triggerTime(scheduleInfoLog.getTriggerTime())
        .attempt(scheduleInfoLog.getAttempt()).scheduleRunId(scheduleInfoLog.getScheduleRunId())
        .requestId(scheduleInfoLog.getRequestId()).build();
  }
}
