package com.flipkart.fdp.superbi.entities;

import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;
import java.util.Date;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import lombok.Data;

/**
 * Created by akshaya.sharma on 08/07/19
 */
@Data
@MappedSuperclass
public abstract class AbstractBaseEntity implements BaseEntity{
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

  @PrePersist
  protected void onCreate() {
    created = new Date();
    updated = created;
  }

  @PreUpdate
  protected void onUpdate() {
    updated = new Date();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AbstractBaseEntity baseData = (AbstractBaseEntity) o;

    if (id != baseData.id) return false;
    if (created != null ? !created.equals(baseData.created) : baseData.created != null) return false;
    if (updated != null ? !updated.equals(baseData.updated) : baseData.updated != null) return false;

    return true;
  }

//  @Override
//  public int hashCode() {
//    int result = (int) (id ^ (id >>> 32));
//    result = 31 * result + (created != null ? created.hashCode() : 0);
//    result = 31 * result + (updated != null ? updated.hashCode() : 0);
//    return result;
//  }
}
