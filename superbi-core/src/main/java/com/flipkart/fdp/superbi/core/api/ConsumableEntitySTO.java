package com.flipkart.fdp.superbi.core.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.fdp.dao.common.service.ServiceTransferObject;
import com.flipkart.fdp.superbi.constants.ConsumableEntityType;
import com.flipkart.fdp.superbi.entities.ConsumableEntity;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.SuperBuilder;

/**
 * Created by akshaya.sharma on 09/07/19
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@AllArgsConstructor
public abstract class ConsumableEntitySTO<T extends ConsumableEntity> extends ServiceTransferObject<T>{
  private String org;
  private String namespace;
  private String name;
  private String displayName;
  private String description;
  private ConsumableEntityType resourceType;
  private List<String> tags;
  private String message;
}
