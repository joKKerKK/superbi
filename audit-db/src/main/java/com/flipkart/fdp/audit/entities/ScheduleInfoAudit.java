package com.flipkart.fdp.audit.entities;


import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "schedule_info_audit")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScheduleInfoAudit {

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Column(name = "trigger_time",nullable = false)
  private Date triggerTime;

  @Column(name = "end_at",nullable = false)
  private Date endAt;

  @Column(name = "schedule_id", nullable = false)
  private long scheduleId;

  @Column(name = "schedule_status", nullable = false)
  private int scheduleStatus;

  @Column(name = "message")
  private String message;

  @Column(name = "schedule_run_id", nullable = false)
  private String scheduleRunId;

  @Column(name = "attempt",nullable = false)
  private Integer attempt = 0;

  @Column(name="request_id")
  private String requestId;

}
