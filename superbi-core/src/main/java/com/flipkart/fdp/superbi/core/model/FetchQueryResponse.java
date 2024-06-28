package com.flipkart.fdp.superbi.core.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 18/06/19
 */
@Getter
@AllArgsConstructor
public abstract class FetchQueryResponse {
  private final Map<String, String> appliedFilters;
}
