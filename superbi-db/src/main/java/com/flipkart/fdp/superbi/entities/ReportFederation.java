package com.flipkart.fdp.superbi.entities;

import com.flipkart.fdp.superbi.converter.MapPropConverter;
import java.io.Serializable;
import java.util.Map;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by akshaya.sharma on 19/07/19
 */
@Entity
@Inheritance(strategy = InheritanceType.JOINED)
@Table(name = "gcp_federation")
@Data
@NoArgsConstructor
@AllArgsConstructor
@IdClass(value = ReportFederation.IdClass.class)
public class ReportFederation {
  @Column(name = "fact_name", nullable = false)
  @Id
  private String factName;

  @Column(name = "report_name", nullable = false)
  @Id
  private String reportName;

  @Column(name = "report_action", nullable = false)
  @Id
  @Enumerated(EnumType.STRING)
  private ReportAction reportAction;

  @Column(name = "overriding_store_identifier")
  private String overriding_store_identifier;

  @Convert(converter = MapPropConverter.class)
  @Column(name = "federation_properties")
  private Map<String,String> federationProperties;


  public ReportFederation(String factName, String reportName, ReportAction reportAction) {
    this(factName,reportName,reportAction,null,null);
  }

  @Data
  public static class IdClass implements Serializable {
    private String factName;
    private String reportName;
    private ReportAction reportAction;
  }
}
