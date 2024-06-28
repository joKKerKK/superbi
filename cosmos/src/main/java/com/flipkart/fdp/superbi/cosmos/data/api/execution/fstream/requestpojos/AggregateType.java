package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Getter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "value"
})
@Getter
public class AggregateType {

  @JsonProperty("value")
  private String value;

  @Builder
  @JsonCreator
  public AggregateType(@JsonProperty("value") String value) {
    this.value = value;
  }

}