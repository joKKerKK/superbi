package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos;

import com.fasterxml.jackson.annotation.*;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@EqualsAndHashCode
public class FstreamRequest {

  @JsonProperty("groupByDimensions")
  @JsonIgnore
  private List<String> groupByDimensions = null;
  @JsonProperty("orderByDimensions")
  @JsonIgnore
  private List<String> orderByDimensions = null;
  @JsonProperty("filters")
  @JsonIgnore
  private Map<String, Object> filters = null;
  @JsonProperty("range")
  private Range range;
  @JsonProperty("aggregates")
  private List<Aggregate> aggregates = null;
  @JsonProperty("in")
  private Map<String,List<Object>> in = null;

  @JsonIgnore
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  public FstreamRequest(List<String> groupByDimensions, List<String> orderByDimensions,
                        Map<String, Object> filters, Range range, List<Aggregate> aggregates,Map<String,List<Object>> in) {
    this.groupByDimensions = groupByDimensions;
    this.orderByDimensions = orderByDimensions;
    this.filters = filters;
    this.range = range;
    this.aggregates = aggregates;
    this.in = in;
  }

  @JsonProperty("groupByDimensions")
  public List<String> getGroupByDimensions() {
    return groupByDimensions;
  }

  @JsonProperty("groupByDimensions")
  public void setGroupByDimensions(List<String> groupByDimensions) {
    this.groupByDimensions = groupByDimensions;
  }

  @JsonProperty("orderByDimensions")
  public List<String> getOrderByDimensions() {
    return orderByDimensions;
  }

  @JsonProperty("orderByDimensions")
  public void setOrderByDimensions(List<String> orderByDimensions) {
    this.orderByDimensions = orderByDimensions;
  }

  @JsonProperty("filters")
  public Map<String, Object> getFilters() {
    return filters;
  }

  @JsonProperty("filters")
  public void setFilters(Map<String, Object> filters) {
    this.filters = filters;
  }

  @JsonProperty("range")
  public Range getRange() {
    return range;
  }

  @JsonProperty("range")
  public void setRange(Range range) {
    this.range = range;
  }

  @JsonProperty("aggregates")
  public List<Aggregate> getAggregates() {
    return aggregates;
  }

  @JsonProperty("aggregates")
  public void setAggregates(List<Aggregate> aggregates) {
    this.aggregates = aggregates;
  }

}