package com.flipkart.fdp.superbi.subscription.model.audit;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ScheduleInfoLog {

  @Getter
  @AllArgsConstructor
  public enum ScheduleStatus{
    FAILURE(0),SUCCESS(1),RETRY(2);
    private int id;
  }

  private Date createdAt;

  private Date triggerTime;

  private Date endAt;

  private long scheduleId;

  private ScheduleStatus scheduleStatus;

  private String message;

  private String scheduleRunId;

  private Integer attempt;

  private String requestId;

}
