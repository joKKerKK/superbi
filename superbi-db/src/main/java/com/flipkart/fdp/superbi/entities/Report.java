package com.flipkart.fdp.superbi.entities;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.Table;
import lombok.Data;

/**
 * Created by akshaya.sharma on 08/07/19
 */

@Entity
@Table(name = "Report_neo")
@DiscriminatorValue("REPORT")
@Data
public class Report extends ConsumableEntity {
  @Column(nullable = true, columnDefinition = "MEDIUMTEXT")
  private String queryFormMetaDetailsJson;

  @Column(nullable = true, columnDefinition = "LONGTEXT")
  private String chartMetaDetailsJson;


  @Column(nullable = true, columnDefinition = "MEDIUMTEXT")
  private String additionalReportAttributes;


  @Column(nullable = true, columnDefinition = "TEXT")
  private String queryStr;

  @Column(nullable = true, columnDefinition = "varchar(255)")
  private String executionEngine;

}
