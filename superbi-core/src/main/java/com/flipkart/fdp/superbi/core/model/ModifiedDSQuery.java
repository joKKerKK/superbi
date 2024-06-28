package com.flipkart.fdp.superbi.core.model;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 17/07/19
 */
@AllArgsConstructor
@Getter
public class ModifiedDSQuery {
  private final DSQuery dsQuery;
  private final Map<String, String> appliedFilters;
}
