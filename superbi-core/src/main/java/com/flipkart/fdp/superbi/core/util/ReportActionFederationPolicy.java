package com.flipkart.fdp.superbi.core.util;

import com.flipkart.fdp.superbi.entities.ReportAction;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import java.util.Comparator;

/**
 * Created by akshaya.sharma on 19/07/19
 */

public class ReportActionFederationPolicy implements Comparator<ReportFederation> {
  private static enum COMPARE_RESULT {NONE, FIRST, SECOND, BOTH};

  @Override
  public int compare(ReportFederation reportFederation1, ReportFederation reportFederation2) {

    COMPARE_RESULT reportNameComparison = compareReportName(reportFederation1, reportFederation2);
    if(COMPARE_RESULT.FIRST == reportNameComparison || COMPARE_RESULT.SECOND == reportNameComparison) {
      return transformToInt(reportNameComparison);
    }

    COMPARE_RESULT reportNSComparison = compareReportNameSpace(reportFederation1,reportFederation2);
    if(COMPARE_RESULT.FIRST == reportNSComparison || COMPARE_RESULT.SECOND == reportNSComparison) {
      return transformToInt(reportNSComparison);
    }

    COMPARE_RESULT reportOrgComparison = compareReportOrg(reportFederation1,reportFederation2);
    if(COMPARE_RESULT.FIRST == reportOrgComparison || COMPARE_RESULT.SECOND == reportOrgComparison) {
      return transformToInt(reportOrgComparison);
    }


    COMPARE_RESULT reportActionNameComparison = compareReportActionName(reportFederation1, reportFederation2);
    if(COMPARE_RESULT.FIRST == reportActionNameComparison || COMPARE_RESULT.SECOND == reportActionNameComparison) {
      return transformToInt(reportActionNameComparison);
    }

    return 0;
  }

  private COMPARE_RESULT compareReportOrg(ReportFederation reportFederation1,
      ReportFederation reportFederation2) {
    boolean first = isReportOrgExact(reportFederation1);
    boolean second = isReportOrgExact(reportFederation2);
    return compareBooleans(first, second);
  }

  private COMPARE_RESULT compareReportNameSpace(ReportFederation reportFederation1,
      ReportFederation reportFederation2) {
    boolean first = isReportNSExact(reportFederation1);
    boolean second = isReportNSExact(reportFederation2);
    return compareBooleans(first, second);
  }

  private boolean isReportNSExact(ReportFederation reportFederation) {
    // org/ns/*
    String[] parts = reportFederation.getReportName().split("/");
    if ( parts.length == 3 ) {
      return "*".equals(parts[2]);
    }
    return false;
  }

  private boolean isReportOrgExact(ReportFederation reportFederation) {
    // org/*
    String[] parts = reportFederation.getReportName().split("/");
    if ( parts.length == 2 ) {
      return "*".equals(parts[1]);
    }
    return false;
  }

  private int transformToInt(COMPARE_RESULT result) {
    if(COMPARE_RESULT.FIRST == result) {
      return -1;
    }else if(COMPARE_RESULT.SECOND == result) {
      return 1;
    }
    return 0;
  }

  private boolean isReportNameExact(ReportFederation reportFederation) {
    // org/ns/report_name
    String[] parts = reportFederation.getReportName().split("/");
    if ( parts.length == 3 ) {
      return !"*".equals(parts[2]);
    }
    return false;
  }

  private boolean isReportActionNameExact(ReportFederation reportFederation) {
    return ReportAction.ALL != reportFederation.getReportAction();
  }

  private COMPARE_RESULT compareReportName(ReportFederation reportFederation1, ReportFederation reportFederation2) {
    boolean first = isReportNameExact(reportFederation1);
    boolean second = isReportNameExact(reportFederation2);
    return compareBooleans(first, second);
  }

  private COMPARE_RESULT compareReportActionName(ReportFederation reportFederation1, ReportFederation reportFederation2) {
    boolean first = isReportActionNameExact(reportFederation1);
    boolean second = isReportActionNameExact(reportFederation2);
    return compareBooleans(first, second);
  }

  private COMPARE_RESULT compareBooleans(boolean first, boolean second) {
    if(first && !second) {
      return COMPARE_RESULT.FIRST;
    }else if(!first && second) {
      return COMPARE_RESULT.SECOND;
    } else if(first && second) {
      return COMPARE_RESULT.BOTH;
    }
    return COMPARE_RESULT.NONE;
  }
}