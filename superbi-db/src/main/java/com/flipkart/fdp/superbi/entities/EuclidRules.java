package com.flipkart.fdp.superbi.entities;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by: mansi.jain on 07/09/23.
 */
@Entity
@Table(name = "euclid_rules")
@Data
@NoArgsConstructor
@AllArgsConstructor
@IdClass(value = EuclidRules.IdClass.class)
public class EuclidRules {

  @Column(name = "fact_name", nullable = false)
  @Id
  private String factName;

  @Column(name = "rule_name")
  @Id
  private String ruleName;

  @Column(name = "rule_definition")
  @Id
  private String ruleDefinition;

  @Data
  public static class IdClass implements Serializable {
    private String factName;
    private String ruleName;
  }

}
