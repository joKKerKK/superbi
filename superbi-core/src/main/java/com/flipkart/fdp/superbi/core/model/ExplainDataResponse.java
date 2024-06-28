package com.flipkart.fdp.superbi.core.model;

import com.codahale.metrics.annotation.Metered;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ExplainDataResponse extends FetchQueryResponse {
  private final Object nativeQuery;
  private final Object nativeQueryExplanation;

  @Builder
  @Metered
  public ExplainDataResponse(Map<String, String> appliedFilters, Object nativeQuery,
      Object nativeQueryExplanation) {
    super(appliedFilters);
    this.nativeQuery = nativeQuery;
    this.nativeQueryExplanation = nativeQueryExplanation;
  }
}
