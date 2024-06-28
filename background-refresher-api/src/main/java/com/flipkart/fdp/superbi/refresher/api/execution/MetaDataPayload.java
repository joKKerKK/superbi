package com.flipkart.fdp.superbi.refresher.api.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Builder
@Getter
public class MetaDataPayload {
  private String username;
  private String client;
  private String reportName;
  private String factName;
  private Map<String,String> executionEngineLabels;

  @JsonCreator
  public MetaDataPayload(@JsonProperty("username")String username,@JsonProperty("client")String client,
                         @JsonProperty("reportName")String reportName, @JsonProperty("factName")String factName,
                         @JsonProperty("executionEngineLabels") Map<String,String> executionEngineLabels) {
    this.username = username;
    this.client = client;
    this.reportName = reportName != null ? reportName : "NA";
    this.factName = factName != null ? factName : "NA";
    this.executionEngineLabels = executionEngineLabels;

  }

  @Override
  public String toString() {
    return "MetaDataPayload{" +
            "username='" + username + '\'' +
            ", client='" + client + '\'' +
            ", reportName='" + reportName + '\'' +
            ", factName='" + factName + '\'' +
            ", executionEngineLabels=" + executionEngineLabels +
            '}';
  }
}
