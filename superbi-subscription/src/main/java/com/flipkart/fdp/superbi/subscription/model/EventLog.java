package com.flipkart.fdp.superbi.subscription.model;

import java.util.Date;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventLog {

  public enum Event{
    JOB,DATA_CALL,D42_UPLOAD,EMAIL,FTP_UPLOAD,SFTP_UPLOAD,CSV_GENERATE,GSHEET_CREATE,
    GSHEET_OVERWRITE
  }

  public enum State{
    COMPLETED,STARTED,FAILED,RETRY,CANCELLED
  }


  private long scheduleId;

  private String scheduleRunId;

  private Integer attempt;

  private Event event;

  private String owner;

  private String message;

  private State state;

  private String org;

  private String namespace;

  private String reportName;

  private Date startedAt;

  private Date completedAt;

  private Date createdAt;

  private Date updatedAt;

  private String scheduleName;

  private String content;

  private boolean isOTS;

}
