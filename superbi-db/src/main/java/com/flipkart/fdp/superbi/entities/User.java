package com.flipkart.fdp.superbi.entities;

import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;
import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

/**
 * Created by akshaya.sharma on 08/07/19
 */
@Entity(name = "HydraUser")
@Table(
    name = "User",
    uniqueConstraints = @UniqueConstraint(columnNames = {"userName"})
)
public class User implements BaseEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private long userId;

  @Column(nullable = false)
  private String userName;

  @Column
  String userEmailId;

  @Lob
  private String preferencesJson;

  @Override
  public Serializable getPK() {
    return userId;
  }
}
