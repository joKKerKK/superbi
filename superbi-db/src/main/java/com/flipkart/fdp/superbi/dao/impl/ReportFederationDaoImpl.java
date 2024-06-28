package com.flipkart.fdp.superbi.dao.impl;

import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAOImpl;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.dao.ReportFederationDao;
import com.flipkart.fdp.superbi.entities.ReportAction;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 19/07/19
 */

public class ReportFederationDaoImpl extends GenericDAOImpl<ReportFederation, ReportFederation
    .IdClass> implements ReportFederationDao {

  @Inject
  protected ReportFederationDaoImpl(
      @Named("HYDRA") GetEntityManagerFunction<GenericDAO, EntityManager> entityManagerFunction) {
    super(entityManagerFunction);
  }

  @Override
  public List<ReportFederation> getFederations(String qualifiedReportName, String fromTable, ReportAction reportAction) {
    Preconditions.checkNotNull(reportAction);
    // Federate only VIEW and DOWNLOAD
    Set<ReportAction> federatedActions = Sets.newHashSet(ReportAction.VIEW, ReportAction.DOWNLOAD,ReportAction.SUBSCRIPTION);
    if (!federatedActions.contains(reportAction)) {
      return Lists.newArrayList();
    }

    return filter(new PredicateProvider<ReportFederation>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder,
          Root<ReportFederation> root,
          Filter filter) {
        Predicate sameFactNamePredicate = criteriaBuilder.equal(root.get("factName"), fromTable);
        Predicate exactReportNamePredicate = criteriaBuilder.equal(root.get("reportName"), qualifiedReportName);
        Predicate wildcardReportNamePredicate = criteriaBuilder.equal(root.get("reportName"), "*");

        Predicate exactReportActionPredicate = criteriaBuilder.equal(root.get("reportAction"), reportAction);
        Predicate wildcardReportActionPredicate = criteriaBuilder.equal(root.get("reportAction"), ReportAction.ALL);

        Predicate reportNamePredicate = criteriaBuilder
            .or(exactReportNamePredicate, wildcardReportNamePredicate);

        String[] parts;
        if (StringUtils.isNotBlank(qualifiedReportName)) {
          parts = qualifiedReportName.split("/");
          String wildCardReportNamespace = getWildCardReportNamespace(parts);
          if ( StringUtils.isNotBlank(wildCardReportNamespace)) {
            Predicate wildCardReportNSPredicate = criteriaBuilder
                .equal(root.get("reportName"), wildCardReportNamespace);
            reportNamePredicate = criteriaBuilder.or(reportNamePredicate,wildCardReportNSPredicate);
          }
          String wildCardReportOrg = getWildCardReportOrg(parts);
          if ( StringUtils.isNotBlank(wildCardReportOrg)) {
            Predicate wildCardReportOrgPredicate = criteriaBuilder
                .equal(root.get("reportName"), wildCardReportOrg);
            reportNamePredicate = criteriaBuilder
                .or(reportNamePredicate, wildCardReportOrgPredicate);
          }
        }

        Predicate reportActionPredicate = criteriaBuilder.or(exactReportActionPredicate, wildcardReportActionPredicate);

        return criteriaBuilder.and(sameFactNamePredicate, reportNamePredicate, reportActionPredicate);
      }
    }, null);
  }

  private String getWildCardReportNamespace(String[] parts ) {
    StringBuilder wildCardReportNamespace = new StringBuilder();
    if (ArrayUtils.isNotEmpty(parts) && parts.length == 3) {
        wildCardReportNamespace.append(parts[0]).append("/").append(parts[1]).append("/")
            .append("*");
        return wildCardReportNamespace.toString();
    }
    return StringUtils.EMPTY;
  }


  private String getWildCardReportOrg(String[] parts) {
    StringBuilder wildCardReportOrg = new StringBuilder();
    if (ArrayUtils.isNotEmpty(parts) && parts.length == 3) {
      wildCardReportOrg.append(parts[0]).append("/").append("*");
      return wildCardReportOrg.toString();
    }
    return StringUtils.EMPTY;
  }
}
