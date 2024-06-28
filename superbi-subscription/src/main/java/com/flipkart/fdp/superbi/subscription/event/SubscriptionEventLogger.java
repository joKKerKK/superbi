package com.flipkart.fdp.superbi.subscription.event;

import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.EventLog.Event;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;

public interface SubscriptionEventLogger {

  boolean startEvent(EventLog eventLog);
  boolean updateEventLog(EventLog eventLog);
  void startEventAudit(EventLog eventLog);
  void updateEventLogAudit(EventLog eventLog);
  EventLog.EventLogBuilder initiateEventLogBuilder(ScheduleInfo scheduleInfo,Event event);

}