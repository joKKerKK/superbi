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
@Table(name = "Dashboard_neo")
@DiscriminatorValue("DASHBOARD")
@Data
public class Dashboard extends ConsumableEntity {
  @Column(nullable = false, columnDefinition="LONGTEXT")
  private String tabsJson;
}
