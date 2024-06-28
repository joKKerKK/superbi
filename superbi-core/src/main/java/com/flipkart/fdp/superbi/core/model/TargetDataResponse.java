package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

/**
 * Created by akshaya.sharma on 21/04/20
 */

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetDataResponse extends FetchQueryResponse {
  @JsonProperty
  protected Map<String, List<Object[]>> data = new HashMap<>();

  @JsonProperty
  protected List<TargetDataSeries> dataSeries = Lists.newArrayList();

  @JsonProperty
  protected List<ReportDataResponse> queryResponses = Lists.newArrayList();

  @JsonProperty
  protected List<TargetMapping> targetMappings = Lists.newArrayList();

  @JsonProperty
  protected Map<String, Object> targetAsOfNow = new HashMap<>();

  @JsonIgnore
  protected QueryInfo.DATA_CALL_TYPE dataCallType;

  public TargetDataResponse() {
    super(Maps.newHashMap());
  }

  public TargetDataResponse(Map<String, List<Object[]>> data,
      List<TargetDataSeries> dataSeries,
      List<ReportDataResponse> queryResponses,
      List<TargetMapping> targetMappings,
      Map<String, Object> targetAsOfNow,
      QueryInfo.DATA_CALL_TYPE dataCallType) {
    super(Maps.newHashMap());
    this.data = data;
    this.dataSeries = dataSeries;
    this.queryResponses = queryResponses;
    this.targetMappings = targetMappings;
    this.targetAsOfNow = targetAsOfNow;
    this.dataCallType = dataCallType;
  }

  public TargetDataResponse merge(TargetDataResponse targetDataResponse) {
    if (targetDataResponse != null) {
      this.data.putAll(targetDataResponse.getData());
      this.dataSeries.addAll(targetDataResponse.getDataSeries());
      this.queryResponses.addAll(targetDataResponse.getQueryResponses());
      this.targetMappings.addAll(targetDataResponse.getTargetMappings());
      this.targetAsOfNow.putAll(targetDataResponse.getTargetAsOfNow());
    }
    return this;
  }
}
