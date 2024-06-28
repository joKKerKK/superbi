package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawQueryResultWithSchema{

  private final Schema schema;

  private final List<List<Object>> data;

  private final Map<String, String> dateHistogramMeta;

  @Builder
  @JsonCreator
  public RawQueryResultWithSchema(@JsonProperty("data") List<List<Object>> data,
      @JsonProperty("dateHistogramMeta") Map<String, String> dateHistogramMeta,@JsonProperty("schema") Schema schema) {
    this.data = data;
    this.dateHistogramMeta = dateHistogramMeta;
    this.schema = schema;
  }
}
