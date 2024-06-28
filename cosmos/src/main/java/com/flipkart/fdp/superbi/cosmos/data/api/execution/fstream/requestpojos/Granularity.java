package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos;

import com.fasterxml.jackson.annotation.*;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "value"
})
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@EqualsAndHashCode
public class Granularity {

  @JsonProperty("value")
  private String value;

  @Builder
  @JsonCreator
  public Granularity(@JsonProperty("value") String value) {
    this.value = value;
  }
}