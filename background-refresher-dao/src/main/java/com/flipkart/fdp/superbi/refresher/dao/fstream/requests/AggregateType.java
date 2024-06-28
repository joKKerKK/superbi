package com.flipkart.fdp.superbi.refresher.dao.fstream.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "value"
})
@Data
@EqualsAndHashCode
public class AggregateType {

  @JsonProperty("value")
  private String value;


  @Builder
  @JsonCreator
  public AggregateType(@JsonProperty("value") String value) {
    this.value = value;
  }

}