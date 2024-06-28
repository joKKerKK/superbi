package com.flipkart.fdp.superbi.refresher.dao.fstream.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "startTime",
    "endTime",
    "granularity"
})
@Data
@EqualsAndHashCode
public class Range {

  @JsonProperty("startTime")
  private Long startTime;
  @JsonProperty("endTime")
  private Long endTime;
  @JsonProperty("granularity")
  private Granularity granularity;

  @JsonCreator
  public Range(@JsonProperty("startTime") Long startTime, @JsonProperty("endTime") Long endTime, @JsonProperty("granularity") Granularity granularity) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.granularity = granularity;
  }
}
