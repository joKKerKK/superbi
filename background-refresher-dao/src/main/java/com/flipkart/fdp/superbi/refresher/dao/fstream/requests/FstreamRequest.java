package com.flipkart.fdp.superbi.refresher.dao.fstream.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@NoArgsConstructor
@EqualsAndHashCode
public class FstreamRequest {

  @JsonProperty("groupByDimensions")
  private List<String> groupByDimensions = null;

  @JsonProperty("orderByDimensions")
  private List<String> orderByDimensions = null;

  @JsonProperty("filters")
  private Map<String, Object> filters = null;

  @JsonProperty("range")
  private Range range;

  @JsonProperty("aggregates")
  private List<Aggregate> aggregates = null;

  @JsonProperty("in")
  private Map<String,List<Object>> in = null;


  public FstreamRequest(List<String> groupByDimensions, List<String> orderByDimensions,
                        Map<String, Object> filters, Range range, List<Aggregate> aggregates,Map<String,List<Object>> in) {
    this.groupByDimensions = groupByDimensions;
    this.orderByDimensions = orderByDimensions;
    this.filters = filters;
    this.range = range;
    this.aggregates = aggregates;
    this.in = in;
  }

}