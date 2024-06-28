package com.flipkart.fdp.superbi.core.adaptor;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.es.client.ESQuery.QueryType;
import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.flipkart.fdp.superbi.core.exception.MalformedQueryException;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.BadgerProcessData;
import com.flipkart.fdp.superbi.cosmos.queryTranslator.CosmosNativeQueryTranslator;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClient;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.execution.BackgroundRefresher;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;


/**
 * Created by akshaya.sharma on 04/07/19
 */
@Slf4j
public class BackgroundQueryExecutorAdaptor {

  public static final String DEFAULT_PRIORITY = StringUtils.EMPTY;

  private final BackgroundRefresher backgroundRefresher;
  private final static String CLIENT_ID = "Super-Bi";
  private final Map<String, AbstractDSLConfig> dslConfigMap;
  private final ObjectMapper mapper = new ObjectMapper();
  private final MetricRegistry metricRegistry;
  private final Function<String, Boolean> checkDSQuerySerialization;
  private final Function<String, Boolean> shouldCalculatePriority;
  private final MmgClient mmgClient;
  private final Function<ESQuery.QueryType, Long> getElasticSearchCostBoost;
  private final Function<String, Boolean> factRefreshTimeRequiredProvider;
  private final CosmosNativeQueryTranslator nativeQueryTranslator;
  private final BadgerClient badgerClient;

  @Inject
  public BackgroundQueryExecutorAdaptor(BackgroundRefresher backgroundRefresher,
    Map<String, AbstractDSLConfig> dslConfigMap,
    MetricRegistry metricRegistry,
    Function<String, Boolean> checkDSQuerySerialization,
    Function<String, Boolean> shouldCalculatePriority
    , MmgClient mmgClient,
    Function<QueryType, Long> getElasticSearchCostBoost,
    Function<String, Boolean> factRefreshTimeRequiredProvider, BadgerClient badgerClient) {
    this.backgroundRefresher = backgroundRefresher;
    this.dslConfigMap = dslConfigMap;
    this.metricRegistry = metricRegistry;
    this.checkDSQuerySerialization = checkDSQuerySerialization;
    this.shouldCalculatePriority = shouldCalculatePriority;
    this.mmgClient = mmgClient;
    this.getElasticSearchCostBoost = getElasticSearchCostBoost;
    this.factRefreshTimeRequiredProvider = factRefreshTimeRequiredProvider;
    this.badgerClient = badgerClient;
    this.nativeQueryTranslator = new CosmosNativeQueryTranslator(this.dslConfigMap,
        this.metricRegistry, this.badgerClient);
  }

  public long getFactRefreshTime(String factName, String storeIdentifier) {
    if (!factRefreshTimeRequiredProvider.apply(storeIdentifier)) {
      return 0L;
    }
    Preconditions.checkArgument(StringUtils.isNotBlank(factName));
    try {
      return mmgClient.getFactRefreshTime(factName, storeIdentifier);
    } catch (Exception e) {
      return new Date().getTime();
    }
  }

  public void submitQuery(final AdaptorQueryPayload adaptorQueryPayload) {
    log.info("BackgroundQueryExecutorAdaptor - submitQuery called");
    QueryPayload queryPayload = adaptToQueryPayload(adaptorQueryPayload);
    if (checkDSQuerySerialization.apply(queryPayload.getStoreIdentifier())) {
      queryPayload = DSQueryUtil.checkDSQuerySerialization(queryPayload);
    }
    backgroundRefresher.submitQuery(queryPayload);
  }

  public long getCostBoost(Object nativeQuery) {
    if (nativeQuery == null) {
      return 0;
    }

    /**
     * Cost boost only for TermQuery of ES
     */
    if (nativeQuery instanceof ESQuery) {
      ESQuery castedNativeQuery = (ESQuery) nativeQuery;
      return getElasticSearchCostBoost.apply(castedNativeQuery.getQueryType());
    }

    return 0;
  }

  public String getPriority(final AdaptorQueryPayload adaptorQueryPayload,
      final Object nativeQuery) {
    if (nativeQuery == null || adaptorQueryPayload == null || !shouldCalculatePriority.apply(
        adaptorQueryPayload.getStoreIdentifier())) {
      // default priority
      return DEFAULT_PRIORITY;
    }

    final String reportAction = adaptorQueryPayload.getReportAction();
    if (StringUtils.isBlank(reportAction)) {
      // default priority
      return DEFAULT_PRIORITY;
    }

    /**
     * Priority differentiation only for ES
     */
    if (!(nativeQuery instanceof ESQuery)) {
      // Default Priority
      return DEFAULT_PRIORITY;
    }

    ESQuery castedNativeQuery = (ESQuery) nativeQuery;
    return StringUtils.join(new String[]{castedNativeQuery.getQueryType().name(), reportAction},
        "_");
  }

  public QueryPayload adaptToQueryPayload(final AdaptorQueryPayload adaptorQueryPayload) {
    NativeQuery nativeQuery = adaptorQueryPayload.getNativeQuery();

    long costBoost = getCostBoost(nativeQuery.getQuery());
    String priority = getPriority(adaptorQueryPayload, nativeQuery.getQuery());

    return QueryPayload.builder()
        .cacheKey(adaptorQueryPayload.getCacheKey())
        .attemptKey(adaptorQueryPayload.getAttemptKey())
        .clientId(CLIENT_ID)
        .deadLine(adaptorQueryPayload.getDeadLine())
        .dsQuery(adaptorQueryPayload.getDsQuery()) // Cosmos lib needs
        .params(adaptorQueryPayload.getParams())  // Cosmos lib needs
        .nativeQuery(nativeQuery)
        .priority(priority)
        .queryWeight(adaptorQueryPayload.getQueryWeight() + costBoost)
        .storeIdentifier(adaptorQueryPayload.getStoreIdentifier())
        .requestId(adaptorQueryPayload.getRequestId())
        .dateRange(adaptorQueryPayload.getDateRange())
        .metaDataPayload(adaptorQueryPayload.getMetaDataPayload())
        .build();
  }

  public NativeQuery getTranslatedQuery(final String storeIdentifier, final DSQuery query,
      final Map<String, String[]> params,
      Map<String, String> federationProperties, Function<String, Map<String, String>> getTableProps,
      Function<String, DataSource> getDataSource, List<String> euclidRules) {
    log.info("getTranslatedQuery started");
    try {
      return nativeQueryTranslator.getNativeQuerySource(storeIdentifier, query, params,
           federationProperties, getTableProps, getDataSource, euclidRules);
    } catch (Exception exception) {
      throw new MalformedQueryException(exception);
    }
  }

  public List<BadgerProcessData> getAllActiveProcessDatas(String org, String namespace, String name){
    return badgerClient.getAllActiveProcessData( org, namespace, name);
  }

}
