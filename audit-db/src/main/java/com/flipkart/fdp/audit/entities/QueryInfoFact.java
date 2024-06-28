package com.flipkart.fdp.audit.entities;

import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;
import java.io.Serializable;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by akshaya.sharma on 28/08/19
 */
@Entity
@Table(name = "query_info_fact")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueryInfoFact implements BaseEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id", nullable = false)
  protected long id;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "created_at", nullable = false)
  private Date created;

  @Temporal(TemporalType.TIMESTAMP)
  @Column(name = "updated_at", nullable = false)
  private Date updated;

  @Column(name = "request_id", nullable = false)
  private String requestId;

  @Column(name = "cache_key", nullable = false)
  private String cacheKey;

  @Column(name = "report_org", nullable = false)
  private String reportOrg;

  @Column(name = "report_ns", nullable = false)
  private String reportNamespace;

  @Column(name = "report_name", nullable = false)
  private String reportName;

  @Column(name = "query_execution_id", nullable = false)
  private String queryExecutionId;

  @Column(name = "data_call_type", nullable = false)
  private String dataCallType;

  @Override
  public Serializable getPK() {
    return id;
  }
}
