package com.flipkart.fdp.superbi.core.service;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.fdp.auth.common.exception.NotAuthorizedException;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.dao.common.service.EntityService;
import com.flipkart.fdp.mmg.cosmos.entities.Source;
import com.flipkart.fdp.mmg.cosmos.entities.SourceType;
import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.core.config.DataPrivilege;
import com.flipkart.fdp.superbi.core.context.ContextProvider;
import com.flipkart.fdp.superbi.core.exception.UnMappedUserException;
import com.flipkart.fdp.superbi.core.model.DownloadResponse;
import com.flipkart.fdp.superbi.core.model.ExplainDataResponse;
import com.flipkart.fdp.superbi.core.model.FetchQueryResponse;
import com.flipkart.fdp.superbi.core.sto.ReportSTOFactory;
import com.flipkart.fdp.superbi.core.util.DSQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.util.MapUtil;
import com.flipkart.fdp.superbi.dao.ReportDao;
import com.flipkart.fdp.superbi.entities.Report;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.qaas.QaasClient;
import com.flipkart.fdp.superbi.http.client.qaas.QaasDownloadResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.stat.Statistics;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.ws.rs.container.ContainerRequestContext;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ReportService extends EntityService<Report, Long,
        ReportSTO, ReportDao> {

  private static final String DASHBOARD_ORG_PARAM = "dashboardOrg";
  private static final String FACTNAME = "factname";
  private static final String DASHBOARD_NAMESPACE_PARAM = "dashboardNamespace";
  private static final String DASHBOARD_NAME_PARAM = "dashboardName";
  public static final String REQUEST_ID = "X-Request-Id";

  private final Map<String, Integer> sourceLimitMap;
  private final DataService dataService;
  private final GringottsClient gringottsClient;
  private final QaasClient qaasClient;

  private final MetricRegistry metricRegistry;

  private static final String SUPERBI_COSMOS_READ_METRIC = "hibernate.superbi.cosmos_read";
  private static final String SUPERBI_MMG_READ_METRIC = "hibernate.superbi.cosmos_mmg_read";

  @Inject
  public ReportService(ReportDao dao,
                       ReportSTOFactory factory,
                       @Named("SourceLimitProvider") Map<String, Integer> sourceLimitMap,
                       DataService dataService, GringottsClient gringottsClient, QaasClient qaasClient, MetricRegistry metricRegistry) {
    super(dao, factory);
    this.sourceLimitMap = sourceLimitMap;
    this.dataService = dataService;
    this.gringottsClient = gringottsClient;
    this.qaasClient = qaasClient;
    this.metricRegistry = metricRegistry;
  }

  @SneakyThrows
  public FetchQueryResponse getReportData(String org, String namespace, String name,
                                          Map<String, String[]> params, ContainerRequestContext requestContext) {
    ReportSTO reportSTO = find(org, namespace, name);
    requestContext.setProperty(FACTNAME, reportSTO.getQueryFormMetaDetailsMap().getFromTable());
    ClientPrivilege clientPrivilege = ContextProvider.getCurrentSuperBiContext()
            .getClientPrivilege();
    Optional<ReportFederation> reportFederation = dataService
            .getFederationForReport(reportSTO, clientPrivilege.getReportAction());
    try {
      return getReportData(reportSTO, params, reportFederation, clientPrivilege.getDataPrivilege());
    } finally {
      // Pushing metrics for cosmos_read
      Statistics cosmosReadStatistics = MetaAccessor.get().getSessionFactory().getStatistics();
      registerHibernateMetrics(metricRegistry, cosmosReadStatistics, SUPERBI_COSMOS_READ_METRIC);

      // Pushing metrics for cosmos_mmg_read
      Statistics cosmosMmgReadStatistics = dataService.getHibernateStatistics();
      registerHibernateMetrics(metricRegistry, cosmosMmgReadStatistics, SUPERBI_MMG_READ_METRIC);
    }
  }

  public ReportSTO find(String org, String namespace, String name) {
    return factory.toSTO(findEntity(org, namespace, name));
  }

  @SneakyThrows
  public FetchQueryResponse getReportData(ReportSTO reportSTO, Map<String, String[]> params,
                                          Optional<ReportFederation> reportFederation, DataPrivilege dataPrivilege) {
    Preconditions.checkNotNull(reportSTO);
    long currentTime = new Date().getTime();

    final String storeIdentifier = dataService
            .getStoreIdentifierForFact(reportSTO.getQueryFormMetaDetailsMap().getFromTable(), reportFederation,reportSTO.getExecutionEngine());

    final DSQueryBuilder.QueryAndParam queryAndParamOp = dataService
            .getQueryAndParam(reportSTO.getQueryFormMetaDetailsMap(), params, dataPrivilege
                    , sourceLimitMap.get(storeIdentifier));


    //TODO:: ContextProvider should not be in superbi-core, it can remain in superbi-web
    final String userName = ContextProvider.getCurrentSuperBiContext().getUserName();
    final Optional<String> systemUserName = ContextProvider.getCurrentSuperBiContext().getSystemUser();
    //TODO::this should be in authorization filter at the resource level

    String resourceOrg = reportSTO.getOrg();
    String resourceNamespace = reportSTO.getNamespace();
    String resourceName = reportSTO.getName();
    String resourceType = "REPORT";
    String privilege = "READ";

    if (params.containsKey(DASHBOARD_ORG_PARAM) && params.containsKey(
            DASHBOARD_NAMESPACE_PARAM) && params.containsKey(DASHBOARD_NAME_PARAM)) {
      resourceOrg = params.get(DASHBOARD_ORG_PARAM)[0];
      resourceNamespace = params.get(DASHBOARD_NAMESPACE_PARAM)[0];
      resourceName = params.get(DASHBOARD_NAME_PARAM)[0];
      resourceType = "DASHBOARD";
      privilege = "READ";
    }

    boolean orgNsMappingPresent =
            gringottsClient.getBillingLabels(userName) == null ? false : true;

    // Will now be pulled out for every case
    boolean newUserTeamOrgNsMappingPresent = gringottsClient.getNewBillingLabels(userName) == null ? false : true;;

    if (!orgNsMappingPresent) {
      // check if system user is present and is mapped
      if (systemUserName.isPresent()) {
        orgNsMappingPresent = gringottsClient.getBillingLabels(systemUserName.get()) == null ? false : true;
      }
      if (!orgNsMappingPresent) {
        // Since old User->Org/Ns mapping is not present, check new user->team->org/ns mapping

        // Both mapping not present, throw an error
        if( !newUserTeamOrgNsMappingPresent ) {
          throw new UnMappedUserException("You are not mapped to any org/namespace.");
        }

      }
    }
    boolean hasOldAccess = false, hasNewAccess = false;
    if(orgNsMappingPresent ) {
      hasOldAccess = gringottsClient.hasPrivillege(userName, resourceOrg,
              resourceNamespace, resourceName, resourceType, privilege);

      if (!hasOldAccess) {
        // check if system user is present and has access
        if (systemUserName.isPresent()) {
          hasOldAccess = gringottsClient.hasPrivillege(systemUserName.get(), resourceOrg,
                  resourceNamespace, resourceName, resourceType, privilege);
        }
      }
    }
    if( newUserTeamOrgNsMappingPresent ) {
      hasNewAccess = gringottsClient.hasNewPrivilege(userName, resourceOrg,
              resourceNamespace, resourceName, resourceType, privilege);
      if (!hasNewAccess) {
        // check if system user is present and has access
        if (systemUserName.isPresent()) {
          log.info("User mapped through team to Org/NS : " + systemUserName.toString());
          hasNewAccess = gringottsClient.hasNewPrivilege(systemUserName.get(), resourceOrg,
                  resourceNamespace, resourceName, resourceType, privilege);
        }
      }
    }

    // Both access are not present, throw error
    if(!hasOldAccess && !hasNewAccess) throw new NotAuthorizedException("You are not authorised to perform action for the report");

    return dataService
            .getResultFromQueryPanelResponse(reportSTO.getQueryFormMetaDetailsMap(), currentTime,
                    params, storeIdentifier,
                    queryAndParamOp,
                    userName, reportFederation.map(ReportFederation::getFederationProperties).orElse(
                            Maps.newHashMap()), reportSTO.getName());
  }

  @SneakyThrows
  public FetchQueryResponse getNativeQuery(String org, String namespace, String name,
                                           Map<String, String[]> params) {
    ReportSTO reportSTO = find(org, namespace, name);
    ClientPrivilege clientPrivilege = ContextProvider.getCurrentSuperBiContext()
            .getClientPrivilege();
    Optional<ReportFederation> reportFederation = dataService
            .getFederationForReport(reportSTO, clientPrivilege.getReportAction());
    Preconditions.checkNotNull(reportSTO);
    final String storeIdentifier = dataService
            .getStoreIdentifierForFact(reportSTO.getQueryFormMetaDetailsMap().getFromTable(), reportFederation,reportSTO.getExecutionEngine());

    final DSQueryBuilder.QueryAndParam queryAndParamOp = dataService
            .getQueryAndParam(reportSTO.getQueryFormMetaDetailsMap(), params,
                    clientPrivilege.getDataPrivilege(), sourceLimitMap.get(storeIdentifier));
    final String userName = ContextProvider.getCurrentSuperBiContext().getUserName();

    final boolean orgNsMappingPresent =
            gringottsClient.getBillingLabels(userName) == null ? false : true;

    if(!orgNsMappingPresent) {
      throw new UnMappedUserException("You are not mapped to any org/namespace.");
    }

    final boolean hasAccess = gringottsClient.hasPrivillege(userName, reportSTO.getOrg(),
            reportSTO.getNamespace(), reportSTO.getName(), "REPORT", "READ");
    if (!hasAccess) {
      throw new NotAuthorizedException("You are not authorised to perform action for the report");
    }

    try {
      return dataService
              .getNativeQueryFromQueryPanelResponse(reportSTO.getQueryFormMetaDetailsMap(), params,
                      storeIdentifier, queryAndParamOp,
                      reportFederation.map(ReportFederation::getFederationProperties)
                              .orElse(Maps.newHashMap()));
    } finally {

      // Pushing metrics for cosmos_read
      Statistics cosmosReadStatistics = MetaAccessor.get().getSessionFactory().getStatistics();
      registerHibernateMetrics(metricRegistry, cosmosReadStatistics, SUPERBI_COSMOS_READ_METRIC);

      // Pushing metrics for cosmos_mmg_read
      Statistics cosmosMmgReadStatistics = dataService.getHibernateStatistics();
      registerHibernateMetrics(metricRegistry, cosmosMmgReadStatistics, SUPERBI_MMG_READ_METRIC);
    }
  }

  private void registerHibernateMetrics(MetricRegistry metricRegistry, Statistics statistics, String prefix) {
    metricRegistry.gauge(prefix + ".cache.hit", () -> statistics::getQueryCacheHitCount);
    metricRegistry.gauge(prefix + ".cache.miss", () -> statistics::getQueryCacheMissCount);
    metricRegistry.gauge(prefix + ".connections", () -> statistics::getConnectCount);
    metricRegistry.gauge(prefix + ".entity_count", () -> statistics::getEntityFetchCount);
  }

  @Override
  public String getIdFieldName() {
    return "id";
  }

  @Timed
  public Report findEntity(String org, String namespace, String name) {
    Preconditions.checkArgument(StringUtils.isNotBlank(org));
    Preconditions.checkArgument(StringUtils.isNotBlank(namespace));
    Preconditions.checkArgument(StringUtils.isNotBlank(name));

    return filterOneEntity(new PredicateProvider<Report>() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder, Root<Report> root,
                                        Filter filter) {
        List<Predicate> predicates = Lists.newArrayList();

        predicates.add(criteriaBuilder.equal(root.get("org"), org));
        predicates.add(criteriaBuilder.equal(root.get("namespace"), namespace));
        predicates.add(criteriaBuilder.equal(root.get("name"), name));

        return criteriaBuilder.and(predicates.toArray(new Predicate[]{}));
      }
    }, null);
  }

  @Override
  public PredicateProvider getDefaultScope() {
    return new PredicateProvider() {
      @Override
      protected Predicate _getPredicate(CriteriaBuilder criteriaBuilder, Root root,
                                        Filter filter) {
        return criteriaBuilder.equal(root.get("disabled"), false);
      }
    };
  }

  @SneakyThrows
  public DownloadResponse downloadReport(String org, String namespace, String reportName, Map<String, String[]> requestParams) {
    ReportSTO reportSTO = find(org, namespace, reportName);
    String factName = reportSTO.getQueryFormMetaDetailsMap().getFromTable();
    Source source = dataService.getSourceByFactName(factName);
    if(source.getSourceType() != SourceType.HIVE && source.getSourceType() != SourceType.HDFS) {
      throw new ClientSideException(String.format("Invalid execution flow for source : '%s', report: '%s'.", source.getName(), reportName));
    }
    String userName = ContextProvider.getCurrentSuperBiContext().getUserName();
    ExplainDataResponse reportDataResponse = getNativeQuery(reportSTO, requestParams);
    QaasDownloadResponse qaasResponse = qaasClient.getDownloadURLForReport(MapUtil.stringToMap(source.getAttributes()).get("source"), (String) reportDataResponse.getNativeQuery(), userName, SourceType.HIVE.name());
    return DownloadResponse.builder().url(qaasResponse.getUrl()).isRedirect(qaasResponse.isRedirect()).service(qaasResponse.getService()).appliedFilters(reportDataResponse.getAppliedFilters()).build();
  }

  @SneakyThrows
  private ExplainDataResponse getNativeQuery(ReportSTO reportSTO, Map<String, String[]> params) {
    ClientPrivilege clientPrivilege = ContextProvider.getCurrentSuperBiContext().getClientPrivilege();
    Optional<ReportFederation> reportFederation = dataService.getFederationForReport(reportSTO, clientPrivilege.getReportAction());
    Preconditions.checkNotNull(reportSTO);
    final String storeIdentifier = dataService.getStoreIdentifierForFact(reportSTO.getQueryFormMetaDetailsMap().getFromTable(), reportFederation,reportSTO.getExecutionEngine());
    final DSQueryBuilder.QueryAndParam queryAndParamOp = dataService
            .getQueryAndParam(reportSTO.getQueryFormMetaDetailsMap(), params,
                    clientPrivilege.getDataPrivilege(), sourceLimitMap.get(storeIdentifier));
    final String userName = ContextProvider.getCurrentSuperBiContext().getUserName();


    final boolean orgNsMappingPresent =
            gringottsClient.getBillingLabels(userName) == null ? false : true;

    if (!orgNsMappingPresent) {
      throw new UnMappedUserException(
              "You are not mapped to any org/namespace.");
    }

    final boolean hasAccess = gringottsClient.hasPrivillege(userName, reportSTO.getOrg(), reportSTO.getNamespace(),
            reportSTO.getName(), "REPORT", "READ");
    if (!hasAccess) {
      throw new NotAuthorizedException("You are not authorised to perform action for the report");
    }

    return dataService
            .getNativeQueryFromQueryPanelResponse(reportSTO.getQueryFormMetaDetailsMap(), params,
                    storeIdentifier, queryAndParamOp,
                    reportFederation.map(ReportFederation::getFederationProperties)
                            .orElse(Maps.newHashMap()));
  }
}