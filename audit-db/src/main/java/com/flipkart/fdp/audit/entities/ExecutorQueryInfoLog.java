package com.flipkart.fdp.audit.entities;

import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;
import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by akshaya.sharma on 28/08/19
 */

@Entity
@Table(name = "cosmos_query_info_fact")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExecutorQueryInfoLog implements BaseEntity {
  @Id
  @Column(name = "id")
  private String id;

  @Column(name = "source_name")
  private String sourceName;

  @Column(name = "source_type")
  private String sourceType;

  @Column(name = "ds_query")
  private String dsQuery;

  @Column(name = "translated_query")
  private String translatedQuery;

  @Column(name = "start_timestamp_ms")
  private long startTimeStampMs;

  @Column(name = "translation_time_ms")
  private long translationTimeMs;

  @Column(name = "execution_time_ms")
  private long executionTimeMs;

  @Column(name = "total_time_ms")
  private long totalTimeMs;

  @Column(name = "is_completed")
  private boolean isCompleted;

  @Column(name = "is_slow_query")
  private boolean isSlowQuery;

  @Column(name = "message")
  private String message;

  @Column(name = "request_id")
  private String requestId;

  @Column(name = "fact_name")
  private String factName;

  @Column(name = "attempt")
  private int attempt;

  @Column(name = "cache_hit")
  private boolean cacheHit;

  @Override
  public Serializable getPK() {
    return id;
  }
}
