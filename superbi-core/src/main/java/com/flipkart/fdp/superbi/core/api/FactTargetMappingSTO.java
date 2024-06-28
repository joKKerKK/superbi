package com.flipkart.fdp.superbi.core.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.fdp.dao.common.service.ServiceTransferObject;
import com.flipkart.fdp.superbi.entities.FactTargetMapping;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Created by akshaya.sharma on 21/04/20
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@AllArgsConstructor
@Builder
public class FactTargetMappingSTO extends ServiceTransferObject<FactTargetMapping> {
  private String factName;

  private String factColumnName;

  private String targetFactName;

  private String targetColumnName;
}
