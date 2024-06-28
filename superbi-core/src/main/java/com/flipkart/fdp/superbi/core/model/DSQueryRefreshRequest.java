package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Optional;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class DSQueryRefreshRequest extends QueryRefreshRequest {

  private final DSQuery dsQuery;
  private final Map<String, String[]> params;
  private final Map<String, String> dateRange;
  private final Map<String,String> federationProperties;
  private final Optional<String> reportName;


  @Builder
  @JsonCreator
  public DSQueryRefreshRequest(@JsonProperty("storeIdentifier") String storeIdentifier,
                               @JsonProperty("appliedFilters") Map<String, String> appliedFilters,
                               @JsonProperty("dsQuery") DSQuery dsQuery,
                               @JsonProperty("params") Map<String, String[]> params,
                               @JsonProperty("dateRange") Map<String, String> dateRange,
                               @JsonProperty("federationProperties") Map<String,String> federationProperties,
                               @JsonProperty("reportName") Optional<String> reportName) {
    super(storeIdentifier, dsQuery.getFromTable(), appliedFilters);
    this.dsQuery = dsQuery;
    this.params = params;
    this.dateRange = dateRange;
    this.federationProperties = federationProperties;
    this.reportName = reportName;
  }
}
