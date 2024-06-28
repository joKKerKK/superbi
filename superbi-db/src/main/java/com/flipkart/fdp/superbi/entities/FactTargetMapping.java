package com.flipkart.fdp.superbi.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.fdp.dao.common.dao.jpa.BaseEntity;
import java.io.Serializable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by akshaya.sharma on 21/04/20
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@EqualsAndHashCode
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name="fact_target_mapping")
@Entity
public class FactTargetMapping implements BaseEntity {
  @EmbeddedId
  private FactTargetMappingId mappingId;

  @Override
  public Serializable getPK() {
    return mappingId;
  }
}
