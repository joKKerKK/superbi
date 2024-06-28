package com.flipkart.fdp.superbi.refresher.api.result.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 12/07/19
 */
@Getter
public class RawQueryResult {

  private final List<List<Object>> data;

  private final List<String> columns;

  private final Map<String, String> dateHistogramMeta;

  @Builder
  @JsonCreator
  public RawQueryResult(@JsonProperty("data") List<List<Object>> data, @JsonProperty("columns") List<String> columns,
      @JsonProperty("dateHistogramMeta") Map<String, String> dateHistogramMeta) {
    this.data = data;
    this.columns = columns;
    this.dateHistogramMeta = dateHistogramMeta;
  }
}