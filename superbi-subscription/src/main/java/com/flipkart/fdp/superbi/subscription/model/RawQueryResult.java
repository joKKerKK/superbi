package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
public class RawQueryResult {

  private final List<List<Object>> data;

  private final Map<String, String> dateHistogramMeta;

  @Builder
  @JsonCreator
  public RawQueryResult(@JsonProperty("data") List<List<Object>> data,
      @JsonProperty("dateHistogramMeta") Map<String, String> dateHistogramMeta) {
    this.data = data;
    this.dateHistogramMeta = dateHistogramMeta;
  }
}
