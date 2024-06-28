package com.flipkart.fdp.superbi.subscription.event;

import com.flipkart.fdp.superbi.dao.SubscriptionEventDao;
import com.flipkart.fdp.superbi.entities.SubscriptionEvent;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.transaction.WorkUnit;
import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.EventLog.Event;
import com.flipkart.fdp.superbi.subscription.model.EventLog.EventLogBuilder;
import com.flipkart.fdp.superbi.subscription.model.EventLog.State;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.google.inject.Inject;
import java.util.Date;
import com.flipkart.fdp.superbi.subscription.configurations.SubscriptionConfig;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

public class SubscriptionEventLoggerImpl implements SubscriptionEventLogger {

  private final SubscriptionEventDao subscriptionEventDao;
  private final SubscriptionConfig subscriptionConfig;
  private final int requestTimeoutInMillies;
  private final String auditorName;
  @Inject
  public SubscriptionEventLoggerImpl(
      SubscriptionEventDao subscriptionEventDao, SubscriptionConfig subscriptionConfig) {
    this.subscriptionEventDao = subscriptionEventDao;
    this.subscriptionConfig = subscriptionConfig;
    this.requestTimeoutInMillies = subscriptionConfig.getDBAuditorRequestTimeout();
    this.auditorName = SubscriptionEventLoggerImpl.class.getSimpleName();
  }

  @Override
  public boolean startEvent(EventLog eventLog) {
    SubscriptionEvent subscriptionEvent = getSubscriptionEventEntity(eventLog);
    subscriptionEventDao.performInTransaction(new WorkUnit() {
      @Override
      public void perform() {
        subscriptionEventDao.persist(subscriptionEvent);
      }
    });
    return true;
  }

  private SubscriptionEvent getSubscriptionEventEntity(EventLog eventLog) {
    return SubscriptionEvent.builder().attempt(eventLog.getAttempt())
        .createdAt(eventLog.getCreatedAt()).updatedAt(new Date()).event(eventLog.getEvent().toString())
        .owner(eventLog.getOwner()).scheduleId(eventLog.getScheduleId()).scheduleRunId(eventLog.getScheduleRunId())
        .startedAt(eventLog.getStartedAt()).state(eventLog.getState().toString())
        .completedAt(eventLog.getCompletedAt()).message(eventLog.getMessage()).content(eventLog.getContent())
        .org(eventLog.getOrg()).namespace(eventLog.getNamespace()).reportName(eventLog.getReportName())
        .scheduleName(eventLog.getScheduleName()).isOTS(eventLog.isOTS()).build();
  }

  @Override
  public boolean updateEventLog(EventLog eventLog) {
    SubscriptionEvent subscriptionEventUpdated = getSubscriptionEventEntity(eventLog);
    subscriptionEventDao.performInTransaction(new WorkUnit() {
      @Override
      public void perform() {
        SubscriptionEvent subscriptionEvent = subscriptionEventDao.filterOne(
            new PredicateProvider<SubscriptionEvent>() {
                 @Override
                 protected Predicate _getPredicate(CriteriaBuilder
                     criteriaBuilder, Root<SubscriptionEvent> root,
                     Filter filter) {
                   Predicate runIdPredicate = criteriaBuilder.equal(root.get("scheduleRunId")
                       , eventLog.getScheduleRunId());
                   Predicate attemptPredicate = criteriaBuilder.equal(root.get
                       ("attempt"), eventLog.getAttempt());
                   Predicate eventPredicate = criteriaBuilder.equal(root.get
                       ("event"), eventLog.getEvent().toString());
                   return criteriaBuilder.and(runIdPredicate, attemptPredicate,eventPredicate);
                 }
             }
        , null);
        subscriptionEventUpdated.setId(subscriptionEvent.getId());
        subscriptionEventDao.merge(subscriptionEventUpdated);
      }
    });

    return true;
  }

  @Override
  public void updateEventLogAudit(EventLog eventLog) {
    new RemoteCall.Builder<Boolean>(auditorName)
        .withTimeOut(requestTimeoutInMillies)
        .around(new ActualCall<Boolean>() {
          @Override
          public Boolean workUnit() {
            return updateEventLog(eventLog);
          }
        }).executeAsync();
  }

  @Override
  public void startEventAudit(EventLog eventLog) {
    new RemoteCall.Builder<Boolean>(auditorName)
        .withTimeOut(requestTimeoutInMillies)
        .around(new ActualCall<Boolean>() {
          @Override
          public Boolean workUnit() {
            return startEvent(eventLog);
          }
        }).executeAsync();
  }

  @Override
  public EventLogBuilder initiateEventLogBuilder(ScheduleInfo scheduleInfo,Event event) {
    EventLog.EventLogBuilder eventLogBuilder = EventLog.builder().scheduleId(scheduleInfo.getSubscriptionId())
        .scheduleName(scheduleInfo.getSubscriptionName()).owner(scheduleInfo.getOwnerId()).isOTS(scheduleInfo.getIsOTS())
        .scheduleRunId(scheduleInfo.getScheduleRunId()).event(event).attempt(scheduleInfo.getAttempt())
        .org(scheduleInfo.getOrg()).namespace(scheduleInfo.getNamespace()).reportName(scheduleInfo.getReportName())
        .createdAt(new Date()).updatedAt(new Date()).startedAt(new Date()).state(State.STARTED);
    return eventLogBuilder;
  }
}
