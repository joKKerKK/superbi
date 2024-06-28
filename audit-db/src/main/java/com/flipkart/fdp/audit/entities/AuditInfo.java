package com.flipkart.fdp.audit.entities;

import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;

import java.io.Serializable;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "audit_info_fact")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuditInfo implements BaseEntity {

  @Column(name = "DTYPE", nullable = false)
  private String dType;

  @Column(name = "id", nullable = false)
  @Id
  @GeneratedValue(strategy= GenerationType.AUTO)
  private long id;

  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Column(name = "updated_at", nullable = false)
  private Date updatedAt;

  @Column(name = "method_name", nullable = false)
  private String methodName;

  @Column(name = "request_type", nullable = false)
  private String requestType;

  @Column(name = "user_name", nullable = false)
  private String userName;

  @Column(name = "action", nullable = false)
  private String action;

  @Column(name = "entity", nullable = false)
  private String entity;

  @Column(name = "org", nullable = false)
  private String org;

  @Column(name = "namespace", nullable = false)
  private String namespace;

  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "operation_type", nullable = false)
  private int operationType;

  @Column(name = "uri", nullable = false)
  private String uri;

  @Column(name = "json_payload", nullable = false)
  private String jsonPayload;

  @Column(name = "request_ts", nullable = false)
  private long requestTimeStamp;

  @Column(name = "status", nullable = false)
  private int httpStatus;

  @Column(name = "time_taken", nullable = false)
  private long timeTaken;

  @Column(name = "host", nullable = false)
  private String host;

  @Column(name = "context_entity", nullable = true)
  private String contextEntity;

  @Column(name = "trace_id", nullable = true)
  private String traceId;

  @Column(name = "request_id", nullable = false)
  private String requestId;

  @Column(name = "error_message", nullable = false)
  private String errorMessage;

  @Column(name = "fact_name", nullable = false)
  private String factName;


  @Override
  public Serializable getPK() {
    return id;
  }
}
