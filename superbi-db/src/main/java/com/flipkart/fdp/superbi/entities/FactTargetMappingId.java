package com.flipkart.fdp.superbi.entities;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by akshaya.sharma on 21/04/20
 */
@Embeddable
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
@EqualsAndHashCode
public class FactTargetMappingId implements Serializable {
  @Column(name = "fact_name")
  private String factName;

  @Column(name = "fact_column_name")
  private String factColumnName;

  @Column(name = "target_fact_name")
  private String targetFactName;

  @Column(name = "target_column_name")
  private String targetColumnName;
}