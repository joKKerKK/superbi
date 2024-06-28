package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "fieldName",
    "aggregateType"
})
@Getter
@Setter
@NoArgsConstructor
public class Aggregate {

  @JsonProperty("fieldName")
  private String fieldName;
  @JsonProperty("aggregateType")
  private AggregateType aggregateType;

  @Builder
  @JsonCreator
  public Aggregate(@JsonProperty("fieldName")String fieldName, @JsonProperty("aggregateType") AggregateType aggregateType) {
    this.fieldName = fieldName;
    this.aggregateType = aggregateType;
  }
}
