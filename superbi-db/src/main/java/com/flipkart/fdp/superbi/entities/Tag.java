package com.flipkart.fdp.superbi.entities;

import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;
import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import lombok.Data;

/**
 * Created by akshaya.sharma on 08/07/19
 */

/**
 * Entity name is given to avoid duplicate mapping exception which is caused when classes share the same name, error message -
 *    org.hibernate.DuplicateMappingException: The [com.flipkart.fdp.superbi.entities.Tag] and
 *    [com.flipkart.fdp.superbi.cosmos.meta.model.data.Tag] entities share the same JPA entity name: [Tag] which is not allowed!
 */
@Entity(name = "superbi-tag")
@Table(name = "Tag_neo")
@Data
public class Tag implements BaseEntity {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  @Column
  private Long tagId=0L;

  @Column
  private String tagValue;

  @Transient
  @Override
  public Serializable getPK() {
    return tagId;
  }
}
