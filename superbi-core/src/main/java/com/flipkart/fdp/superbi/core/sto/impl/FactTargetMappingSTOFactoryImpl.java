package com.flipkart.fdp.superbi.core.sto.impl;

import com.flipkart.fdp.superbi.core.api.FactTargetMappingSTO;
import com.flipkart.fdp.superbi.core.sto.FactTargetMappingSTOFactory;
import com.flipkart.fdp.superbi.entities.FactTargetMapping;
import com.flipkart.fdp.superbi.entities.FactTargetMappingId;

/**
 * Created by akshaya.sharma on 21/04/20
 */

public class FactTargetMappingSTOFactoryImpl implements FactTargetMappingSTOFactory {
  @Override
  public FactTargetMappingSTO toSTO(FactTargetMapping factTargetMapping) {
    if(factTargetMapping == null) {
      return null;
    }
    FactTargetMappingId factTargetMappingId = factTargetMapping.getMappingId();
    return FactTargetMappingSTO.builder()
        .factName(factTargetMappingId.getFactName())
        .factColumnName(factTargetMappingId.getFactColumnName())
        .targetFactName(factTargetMappingId.getTargetFactName())
        .targetColumnName(factTargetMappingId.getTargetColumnName())
        .build();
  }

  @Override
  public FactTargetMapping toDTO(FactTargetMappingSTO factTargetMappingSTO) {
    if(factTargetMappingSTO == null) {
      return null;
    }
    FactTargetMappingId factTargetMappingId = FactTargetMappingId.builder()
        .factName(factTargetMappingSTO.getFactName())
        .factColumnName(factTargetMappingSTO.getFactColumnName())
        .targetFactName(factTargetMappingSTO.getTargetFactName())
        .targetColumnName(factTargetMappingSTO.getTargetColumnName())
        .build();

    return FactTargetMapping.builder()
        .mappingId(factTargetMappingId)
        .build();
  }

  @Override
  public Class<FactTargetMapping> getDTOClass() {
    return FactTargetMapping.class;
  }

  @Override
  public Class<FactTargetMappingSTO> getSTOClass() {
    return FactTargetMappingSTO.class;
  }
}
