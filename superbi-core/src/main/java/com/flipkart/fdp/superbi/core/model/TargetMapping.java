package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by akshaya.sharma on 22/04/20
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetMapping {
  @JsonProperty
  protected List<String> targetNames;

  @JsonProperty
  protected TargetFact targetFact;

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public class TargetFact {
    @JsonProperty
    protected String name;

    @JsonProperty
    protected List<String> columns;

    @JsonProperty
    protected String baseColumnName;
  }
}
