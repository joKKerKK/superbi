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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "ds_query_info_log")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DSQueryInfoLog implements BaseEntity {

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "ds_query")
  private String dsQuery;

  @Column(name = "request_id")
  private String requestId;

  @Column(name = "created_at", nullable = false)
  private Date createdAt;

  @Override
  public Serializable getPK() {
    return id;
  }
}
