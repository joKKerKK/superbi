package com.flipkart.fdp.superbi.dao;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.superbi.entities.ReportAction;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import java.util.List;

/**
 * Created by akshaya.sharma on 19/07/19
 */

public interface ReportFederationDao extends GenericDAO<ReportFederation, ReportFederation.IdClass> {
  List<ReportFederation> getFederations(String qualifiedReportName, String fromTable, ReportAction reportAction);
}
