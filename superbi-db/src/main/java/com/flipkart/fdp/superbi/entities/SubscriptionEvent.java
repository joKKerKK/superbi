package com.flipkart.fdp.superbi.entities;

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
@Table(name = "subscription_event")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubscriptionEvent {

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "schedule_id", nullable = false)
  private long scheduleId;

  @Column(name = "schedule_name",nullable = false)
  private String scheduleName;

  @Column(name = "content")
  private String content;

  @Column(name = "schedule_run_id",nullable = false)
  private String scheduleRunId;

  @Column(name = "attempt", nullable = false)
  private Integer attempt;

  @Column(name = "event",nullable = false)
  private String event;

  @Column(name = "is_ots",nullable = false)
  private boolean isOTS;

  @Column(name = "owner",nullable = false)
  private String owner;

  @Column(name = "message")
  private String message;

  @Column(name = "state", nullable = false)
  private String state;

  @Column(name = "org")
  private String org;

  @Column(name = "namespace")
  private String namespace;

  @Column(name = "report_name")
  private String reportName;

  @Column(name = "started_at", nullable = false)
  private Date startedAt;

  @Column(name = "completed_at")
  private Date completedAt;

  @Column(name = "created_at")
  private Date createdAt;

  @Column(name = "updated_at")
  private Date updatedAt;




}
