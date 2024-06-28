package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 22/04/20
 */
@Data
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetDataSeries {
  @JsonIgnore
  protected String targetFactName = StringUtils.EMPTY;

  @JsonIgnore
  protected String targetName;

  @JsonIgnore
  protected String metricName;

  protected String label;

  protected String key;

  @JsonProperty
  public String getKey() {
    return StringUtils.isNotBlank(
        key) ? key : targetFactName + " - " + targetName + " - " + metricName;
  }

  @JsonGetter
  public String getLabel() {
    return StringUtils.isNotBlank(label) ? label : getKey();
  }

  @JsonSetter
  public void setLabel(String label) {
    this.label = label;
  }

  public TargetDataSeries(String targetName, String metricName) {
    this("", targetName, metricName, null);
  }

  public TargetDataSeries(String targetFactName, String targetName, String metricName,
      String label) {
    this.targetFactName = targetFactName;
    this.targetName = targetName;
    this.metricName = metricName;
    this.label = label;
  }
}
