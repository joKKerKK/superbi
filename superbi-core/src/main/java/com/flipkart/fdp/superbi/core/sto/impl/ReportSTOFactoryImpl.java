package com.flipkart.fdp.superbi.core.sto.impl;

import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.entities.Report;
import com.flipkart.fdp.superbi.core.sto.ReportSTOFactory;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.util.Map;
import java.util.Optional;

/**
 * Created by akshaya.sharma on 08/07/19
 */

public class ReportSTOFactoryImpl implements ReportSTOFactory {
  @Override
  public ReportSTO toSTO(Report report) {
    if(report == null) {
      return null;
    }

    QueryPanel queryPanel = JsonUtil.fromJsonOptional(report.getQueryFormMetaDetailsJson(), QueryPanel.class).orNull();

    queryPanel.setAssociatedFactColumns();

    Map<String, Object> chartDetails = JsonUtil.fromJsonOptional(report.getChartMetaDetailsJson(), Map.class).orNull();
    Map<String, Object> additionalAttributes = JsonUtil.fromJsonOptional(report.getAdditionalReportAttributes(), Map.class).orNull();
    Optional<String> executionEngine = Optional.ofNullable(report.getExecutionEngine());


    ReportSTO reportSTO = ReportSTO.builder()
        .org(report.getOrg())
        .namespace(report.getNamespace())
        .name(report.getName())
        .displayName(report.getDisplayName())
        .description(report.getDescription())
        .resourceType(report.getResourceType())
//        .tags(report.getTags())
        .message(report.getMessage())
        .additionalReportAttributes(additionalAttributes)
        .chartMetaDetailsMap(chartDetails)
        .queryFormMetaDetailsMap(queryPanel)
        .queryStr(report.getQueryStr())
        .executionEngine(executionEngine)
        .build();

    return reportSTO;
  }

  @Override
  public Report toDTO(ReportSTO reportSTO) {
    return null;
  }

  @Override
  public Class<Report> getDTOClass() {
    return Report.class;
  }

  @Override
  public Class<ReportSTO> getSTOClass() {
    return ReportSTO.class;
  }
}
