package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.cosmos.data.ExecutionEventType;

public class ExecutionEvent {
  private ExecutionEventType eventType;
  private Object eventData;

  public ExecutionEventType getEventType() {
    return eventType;
  }

  public Object getEventData() {
    return eventData;
  }

  public <T> T getEventDataAs() {
    return (T) eventData;
  }

  public ExecutionEvent(ExecutionEventType eventType, Object eventData) {
    this.eventType = eventType;
    this.eventData = eventData;
  }
}
