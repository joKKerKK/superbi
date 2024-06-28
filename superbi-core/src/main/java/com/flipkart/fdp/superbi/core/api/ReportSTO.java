package com.flipkart.fdp.superbi.core.api;

import com.flipkart.fdp.superbi.constants.ConsumableEntityType;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.entities.Report;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

/**
 * Created by akshaya.sharma on 08/07/19
 */

@Data
public class ReportSTO extends ConsumableEntitySTO<Report> {
  private Map<String, Object> chartMetaDetailsMap;
  private QueryPanel queryFormMetaDetailsMap;
  private Map<String, Object> additionalReportAttributes;
  private String queryStr;
  private Optional<String> executionEngine;

  @Builder
  public ReportSTO(String org, String namespace, String name, String displayName,
      String description, ConsumableEntityType resourceType, List<String> tags,
      String message, Map<String, Object> chartMetaDetailsMap,
      QueryPanel queryFormMetaDetailsMap,
      Map<String, Object> additionalReportAttributes, String queryStr,Optional<String> executionEngine) {
    super(org, namespace, name, displayName, description, resourceType, tags, message);
    this.chartMetaDetailsMap = chartMetaDetailsMap;
    this.queryFormMetaDetailsMap = queryFormMetaDetailsMap;
    this.additionalReportAttributes = additionalReportAttributes;
    this.queryStr = queryStr;
    this.executionEngine = executionEngine == null? Optional.empty():executionEngine;
  }

  public final String getQualifiedReportName() {
    StringBuilder builder = new StringBuilder();
    builder.append(getOrg() + "/" + getNamespace());

    if (!getNamespace().equals("*"))
      builder.append("/" + getName());

    return builder.toString();
  }
}
