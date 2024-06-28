package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "startTime",
    "endTime",
    "granularity"
})
@Getter
@NoArgsConstructor
@Setter
@EqualsAndHashCode
public class Range {

  @JsonProperty("startTime")
  private Long startTime;
  @JsonProperty("endTime")
  private Long endTime;
  @JsonProperty("granularity")
  private Granularity granularity;


  @JsonCreator
  @Builder
  public Range(@JsonProperty("startTime")Long startTime, @JsonProperty("endTime") Long endTime, @JsonProperty("granularity") Granularity granularity) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = granularity;
  }
}
