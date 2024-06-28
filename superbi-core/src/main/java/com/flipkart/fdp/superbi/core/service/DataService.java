package com.flipkart.fdp.superbi.core.service;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.fdp.dao.common.dao.jpa.PredicateProvider;
import com.flipkart.fdp.dao.common.jdbc.query.filter.Filter;
import com.flipkart.fdp.mmg.cosmos.dao.DataSourceDao;
import com.flipkart.fdp.mmg.cosmos.dao.DimensionDao;
import com.flipkart.fdp.mmg.cosmos.dao.FactDao;
import com.flipkart.fdp.mmg.cosmos.entities.*;
import com.flipkart.fdp.superbi.core.adaptor.AdaptorQueryPayload;
import com.flipkart.fdp.superbi.core.adaptor.BackgroundQueryExecutorAdaptor;
import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.api.query.PanelEntry;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.core.cache.CacheKeyGenerator;
import com.flipkart.fdp.superbi.core.config.CacheExpiryConfig;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.core.config.DataPrivilege;
import com.flipkart.fdp.superbi.core.config.SuperbiConfig;
import com.flipkart.fdp.superbi.core.context.ContextProvider;
import com.flipkart.fdp.superbi.core.exception.MalformedQueryException;
import com.flipkart.fdp.superbi.core.exception.MalformedReportException;
import com.flipkart.fdp.superbi.core.exception.MissingColumnsException;
import com.flipkart.fdp.superbi.core.exception.PartitionColumnMissingException;
import com.flipkart.fdp.superbi.core.exception.ServiceUnavailableException;
import com.flipkart.fdp.superbi.core.exception.SuperBiRuntimeException;
import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.model.*;
import com.flipkart.fdp.superbi.core.model.QueryInfo.DATA_CALL_TYPE;
import com.flipkart.fdp.superbi.core.util.AuthorizationUtil;
import com.flipkart.fdp.superbi.core.util.DSQueryBuilder;
import com.flipkart.fdp.superbi.core.util.DSQueryBuilder.QueryAndParam;
import com.flipkart.fdp.superbi.core.util.QueryUtil;
import com.flipkart.fdp.superbi.core.util.ReportActionFederationPolicy;
import com.flipkart.fdp.superbi.cosmos.DataSourceUtil;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.BadgerEntityColumn;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.BadgerProcessData;
import com.flipkart.fdp.superbi.cosmos.dsQuery.DsQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.dao.EuclidRulesDao;
import com.flipkart.fdp.superbi.dao.NativeExpressionDao;
import com.flipkart.fdp.superbi.dao.ReportFederationDao;
import com.flipkart.fdp.superbi.dao.TableFederationDao;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.entities.EuclidRules;
import com.flipkart.fdp.superbi.entities.ReportAction;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import com.flipkart.fdp.superbi.entities.TableFederation;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.flipkart.fdp.superbi.refresher.dao.validation.BatchCubeGuardrailConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.jetbrains.annotations.NotNull;
import org.slf4j.MDC;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.flipkart.fdp.superbi.cosmos.utils.Constants.*;

/**
 * Created by akshaya.sharma on 18/06/19
 */
@Slf4j
public class DataService {

  public static final String REQUEST_ID = "X-Request-Id";
  private static final String DATA_STALENESS_METRIC_KEY = "data.staleness.time";
  private static final String DEFAULT = "DEFAULT";
  private static final String EXECUTION_ENGINE = "executionEngine";

  // DATA_HARD_REFRESH is proxied by FORCE_REFRESH_ALL
  private final Map<String, Integer> sourceLimitMap;
  private final BackgroundQueryExecutorAdaptor backgroundQueryExecutorAdaptor;
  private final CacheKeyGenerator cacheKeyGenerator;
  private final CacheDao resultStore;
  private final CacheDao attemptStore;
  private final FactDao factDao;
  private final DimensionDao dimensionDao;
  private final GringottsClient gringottsClient;
  private final SuperbiConfig superbiConfig;
  private final ReportFederationDao reportFederationDao;
  private final TableFederationDao tableFederationDao;
  private final EuclidRulesDao euclidRulesDao;
  private final NativeExpressionDao nativeExpressionDao;
  private final MetricRegistry metricRegistry;
  private final List<String> d42UploadClients;
  private final Auditer auditer;
  private final Map<String, Histogram> dateStalenessHistograms;
  private final DataSourceDao dataSourceDao;
  private static ReportActionFederationPolicy reportActionFederationPolicy = new
      ReportActionFederationPolicy();

  private final ExecutionAuditor executionAuditor;

  private static ImmutableMap<String, String> SOURCE_TYPE_MAP;

  public static final int HIGH_CARDINALITY_LIMIT = 5000;

  private static final String SUPERBI_COSMOS_READ_METRIC = "hibernate.superbi.cosmos_read";
  private static final String SUPERBI_MMG_READ_METRIC = "hibernate.superbi.cosmos_mmg_read";

  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("HDFS", "batch reports");
    builder.put("Vertica", "batch reports");
    builder.put("ElasticSearch", "real time reports");
    SOURCE_TYPE_MAP = builder.build();

  }

  @Inject
  public DataService(
          BackgroundQueryExecutorAdaptor backgroundQueryExecutorAdaptor,
          CacheKeyGenerator cacheKeyGenerator,
          @Named("RESULT_STORE") CacheDao resultStore,
          @Named("ATTEMPT_STORE") CacheDao attemptStore,
          FactDao factDao,
          GringottsClient gringottsClient,
          SuperbiConfig superbiConfig, Auditer auditer,
          ReportFederationDao reportFederationDao, MetricRegistry metricRegistry,
          @Named("SourceLimitProvider") Map<String, Integer> sourceLimitMap, DimensionDao dimensionDao,
          EuclidRulesDao euclidRulesDao, @Named("D42UploadClients") List<String> d42UploadClients,
          DataSourceDao dataSourceDao, TableFederationDao tableFederationDao, NativeExpressionDao nativeExpressionDao, ExecutionAuditor executionAuditor) {
    this.backgroundQueryExecutorAdaptor = backgroundQueryExecutorAdaptor;
    this.cacheKeyGenerator = cacheKeyGenerator;
    this.resultStore = resultStore;
    this.attemptStore = attemptStore;
    this.factDao = factDao;
    this.gringottsClient = gringottsClient;
    this.superbiConfig = superbiConfig;
    this.reportFederationDao = reportFederationDao;
    this.metricRegistry = metricRegistry;
    this.euclidRulesDao = euclidRulesDao;
    this.executionAuditor = executionAuditor;
    this.dateStalenessHistograms = registerDataStalenessHistogramsForSources();
    this.auditer = auditer;
    this.sourceLimitMap = sourceLimitMap;
    this.dimensionDao = dimensionDao;
    this.d42UploadClients = d42UploadClients;
    this.dataSourceDao = dataSourceDao;
    this.tableFederationDao = tableFederationDao;
    this.nativeExpressionDao = nativeExpressionDao;
  }

  @SneakyThrows
  public FetchQueryResponse getLov(String factName, String degDim, Map<String, String[]> params) {
    Preconditions.checkNotNull(factName);
    Preconditions.checkNotNull(degDim);
    Optional<ReportFederation> reportFederation = getFederationFromTable(factName,
        ReportAction.VIEW);
    Optional<String> executionEngine = Optional.empty();
    if(params.containsKey(EXECUTION_ENGINE))
      executionEngine = Optional.ofNullable(params.get(EXECUTION_ENGINE)[0]);
    final String storeIdentifier = getStoreIdentifierForFact(factName, reportFederation, executionEngine);
    DSQuery dsQuery = DsQueryBuilder.getFor(factName, degDim);
    return getLovResponse(storeIdentifier, dsQuery,
        reportFederation.map(ReportFederation::getFederationProperties).orElse(
            Maps.newHashMap()));
  }

  @SneakyThrows
  public FetchQueryResponse getLov(String dimension, String hierarchy, String columnName, Map<String, String[]> params) {
    Preconditions.checkNotNull(dimension);
    Preconditions.checkNotNull(hierarchy);
    Preconditions.checkNotNull(columnName);
    Optional<ReportFederation> reportFederation = getFederationFromTable(dimension,
        ReportAction.VIEW);
    Optional<String> executionEngine = Optional.empty();
    if(params.containsKey(EXECUTION_ENGINE))
      executionEngine = Optional.ofNullable(params.get(EXECUTION_ENGINE)[0]);
    final String storeIdentifier = getStoreIdentifierForDimension(dimension, reportFederation,executionEngine);
    DSQuery dsQuery = DsQueryBuilder.getFor(dimension, hierarchy, columnName);
    return getLovResponse(storeIdentifier, dsQuery,
        reportFederation.map(ReportFederation::getFederationProperties).orElse(
            Maps.newHashMap()));
  }

  private FetchQueryResponse getLovResponse(String storeIdentifier, DSQuery dsQuery, Map<String,String> federationProperties) {
    QueryRefreshRequest queryRefreshRequest = DSQueryRefreshRequest.builder()
        .dsQuery(dsQuery)
        .dateRange(new HashMap<>())
        .params(new HashMap<>())
        .appliedFilters(new HashMap<>())
        .storeIdentifier(storeIdentifier)
        .federationProperties(federationProperties)
        .reportName(Optional.empty())
        .build();

    FetchQueryResponse fetchQueryResponse = executeQuery(queryRefreshRequest,
        new Date().getTime());

    if (fetchQueryResponse instanceof ReportDataResponse) {
      QueryResultCachedValue queryResultCachedValue = ((ReportDataResponse) fetchQueryResponse)
          .getQueryCachedResult();
      if (queryResultCachedValue != null
          && queryResultCachedValue.getTotalNumberOfRows() > HIGH_CARDINALITY_LIMIT) {
        return LovResponse.builder().appliedFilters(fetchQueryResponse.getAppliedFilters())
            .attemptInfo(((ReportDataResponse) fetchQueryResponse).getAttemptInfo())
            .highCardinality(true).build();
      }
    }
    // Pushing metrics for cosmos_read
    Statistics cosmosReadStatistics = MetaAccessor.get().getSessionFactory().getStatistics();
    registerHibernateMetrics(metricRegistry, cosmosReadStatistics, SUPERBI_COSMOS_READ_METRIC);

    // Pushing metrics for cosmos_mmg_read
    Statistics cosmosMmgReadStatistics = getHibernateStatistics();
    registerHibernateMetrics(metricRegistry, cosmosMmgReadStatistics, SUPERBI_MMG_READ_METRIC);

    return fetchQueryResponse;
  }

  public ReportDataResponse executeQuery(QueryRefreshRequest queryRefreshRequest) {
    Preconditions.checkNotNull(queryRefreshRequest);
    return executeQuery(queryRefreshRequest, new Date().getTime());
  }


  @SneakyThrows
  public FetchQueryResponse executeQueryPanel(QueryPanel queryPanel) {
    Preconditions.checkNotNull(queryPanel);

    queryPanel.setAssociatedFactColumns();
    queryPanel.validateQueryPanelAndThrowException();

    long currentTime = new Date().getTime();
    ClientPrivilege clientPrivilege = ContextProvider.getCurrentSuperBiContext()
        .getClientPrivilege();
    String fromTable = queryPanel.getFromTable();

    Optional<ReportFederation> reportFederation = getFederationFromTable(fromTable,
        ReportAction.VIEW);

    final String storeIdentifier = getStoreIdentifierForFact(fromTable, reportFederation,queryPanel.getExecutionEngine());

    Map<String, String[]> params = new HashMap<>();
    // Get translated query
    final DSQueryBuilder.QueryAndParam queryAndParamOp = getQueryAndParam(queryPanel,
        new HashMap<>(), clientPrivilege.getDataPrivilege(), sourceLimitMap.get(storeIdentifier));
    final String userName = ContextProvider.getCurrentSuperBiContext().getUserName();

    try {
      return getResultFromQueryPanelResponse(queryPanel, currentTime, params, storeIdentifier,
              queryAndParamOp,
              userName, reportFederation.map(ReportFederation::getFederationProperties).orElse(
                      Maps.newHashMap()), null);
    } finally {
      // Pushing metrics for cosmos_read
      Statistics cosmosReadStatistics = MetaAccessor.get().getSessionFactory().getStatistics();
      registerHibernateMetrics(metricRegistry, cosmosReadStatistics, SUPERBI_COSMOS_READ_METRIC);

      // Pushing metrics for cosmos_mmg_read
      Statistics cosmosMmgReadStatistics = getHibernateStatistics();
      registerHibernateMetrics(metricRegistry, cosmosMmgReadStatistics, SUPERBI_MMG_READ_METRIC);
    }

  }

  private void registerHibernateMetrics(MetricRegistry metricRegistry, Statistics statistics, String prefix) {
    metricRegistry.gauge(prefix + ".cache.hit", () -> statistics::getQueryCacheHitCount);
    metricRegistry.gauge(prefix + ".cache.miss", () -> statistics::getQueryCacheMissCount);
    metricRegistry.gauge(prefix + ".connections", () -> statistics::getConnectCount);
    metricRegistry.gauge(prefix + ".entity_count", () -> statistics::getEntityFetchCount);
  }

  public FetchQueryResponse getResultFromQueryPanelResponse(QueryPanel queryPanel, long currentTime,
      Map<String, String[]> params, String storeIdentifier, QueryAndParam queryAndParamOp,
      String userName, Map<String,String> federationProperties, String reportName) {
    final Map<String, String> userSecurityAttributes = gringottsClient.getUserSecurityAttributes(
        userName);
    DSQuery dsQuery = queryAndParamOp.getQuery();
    params = QueryUtil.preparePrams(params, queryAndParamOp);
    Fact fact = getFactByName(dsQuery.getFromTable());

    final ModifiedDSQuery modifiedDSQuery = AuthorizationUtil.applySecurityAttributesForUser(
        dsQuery, userSecurityAttributes, fact);
    dsQuery = modifiedDSQuery.getDsQuery();
    final Map<String, String> appliedFilters = modifiedDSQuery.getAppliedFilters();

    List<PanelEntry> groupByColumns = queryPanel.getGroupByColumns();
    List<PanelEntry> filterColumns = queryPanel.getFilterColumns();
    List<PanelEntry> combinedValidationList = new ArrayList<>();
    combinedValidationList.addAll(groupByColumns);
    combinedValidationList.addAll(filterColumns);

    Map<String, String> dateRange = new HashMap<>();

    for (PanelEntry column : groupByColumns) {
      if (column.isDateRangeOrHistogram()) {
        dateRange.put("startTimestamp",
            params.get(column.getFactCol().getFQName() + ".startTimestamp")[0]);
        dateRange.put("endTimestamp",
            params.get(column.getFactCol().getFQName() + ".endTimestamp")[0]);
        dateRange.put("downsample", params.get(column.getFactCol().getFQName() + ".downsample")[0]);
        dateRange.put("seriesType", params.get("seriesType")[0]);
        break;
      }
    }

    boolean foundValidPartition = validatePartitionKey(fact, combinedValidationList);
    if (!foundValidPartition) {
      throw new PartitionColumnMissingException(fact.getName());
    }



    QueryRefreshRequest dsQueryRefreshRequest = DSQueryRefreshRequest.builder()
        .dsQuery(dsQuery)
        .dateRange(dateRange)
        .params(params)
        .appliedFilters(appliedFilters)
        .storeIdentifier(storeIdentifier)
        .federationProperties(federationProperties)
        .reportName(Optional.ofNullable(reportName))
        .build();

    return executeQuery(dsQueryRefreshRequest, currentTime, userName,
        userSecurityAttributes);
  }

  public ExplainDataResponse getNativeQueryFromQueryPanelResponse(QueryPanel queryPanel,
      Map<String, String[]> params, String storeIdentifier, QueryAndParam queryAndParamOp,
      Map<String,String> federationProperties) {
    DSQuery dsQuery = queryAndParamOp.getQuery();
    params = QueryUtil.preparePrams(params, queryAndParamOp);
    List<PanelEntry> groupByColumns = queryPanel.getGroupByColumns();
    Map<String, String> dateRange = new HashMap<>();

    for (PanelEntry column : groupByColumns) {
      if (column.isDateRangeOrHistogram()) {
        dateRange.put("startTimestamp",
            params.get(column.getFactCol().getFQName() + ".startTimestamp")[0]);
        dateRange.put("endTimestamp",
            params.get(column.getFactCol().getFQName() + ".endTimestamp")[0]);
        dateRange.put("downsample", params.get(column.getFactCol().getFQName() + ".downsample")[0]);
        dateRange.put("seriesType", params.get("seriesType")[0]);
        break;
      }
    }
    Table table = getTable(dsQuery.getFromTable());
    return ExplainDataResponse.builder().nativeQuery(
            backgroundQueryExecutorAdaptor.getTranslatedQuery(storeIdentifier, dsQuery, params,
                    modifyFederationProperties(table, federationProperties, storeIdentifier),
                    (tableName) -> getBqProperties(tableName, storeIdentifier),
                    (fromTable) -> getFactByName(fromTable), getRulesForEuclidFact(dsQuery.getFromTable()))
                .getQuery())
        .build();
  }

  private Map<String, String> enrichFederationProperties(String tableName, String storeIdentifier) {
    Map<String, List<String>> tableEnrichStoreIdentifierMap = this.superbiConfig.getStoreIdentifiersForTableEnrich();
    Map<String, String> datasourceConfigs = this.superbiConfig.getDataSourceAttributes().get(storeIdentifier);
    Map<String, String> enrichedFederationProperties = new HashMap<>();
    String enrichedTableName;
    if (tableEnrichStoreIdentifierMap.get(REALTIME_SUPPORTED_STORE_IDENTIFIER_KEY) != null && tableEnrichStoreIdentifierMap.get(REALTIME_SUPPORTED_STORE_IDENTIFIER_KEY).contains(storeIdentifier)) {
      //f_orgName_namespace.tablename -> f_orgName_namespace--tablename
      enrichedTableName = tableName.replaceFirst("\\.", "--");
      enrichedFederationProperties.put("bq.project_id", datasourceConfigs.get(SOURCE_PROJECT_ID_KEY));
      enrichedFederationProperties.put("bq.dataset_name", datasourceConfigs.get(SOURCE_DATASET_NAME_KEY));
      enrichedFederationProperties.put("bq.table_name", enrichedTableName);
    } else if (tableEnrichStoreIdentifierMap.get(BATCH_SUPPORTED_STORE_IDENTIFIER_KEY) != null && tableEnrichStoreIdentifierMap.get(BATCH_SUPPORTED_STORE_IDENTIFIER_KEY).contains(storeIdentifier)) {
      //b_orgName_namespace.tablename -> orgName_namespace__tablename
      if(tableName.endsWith("target_fact")) {
        enrichedTableName = tableName.substring(tableName.indexOf(".") + 1).toLowerCase();
        enrichedFederationProperties.put("bq.project_id",datasourceConfigs.get(SOURCE_PROJECT_ID_KEY_TARGET_FACT));
        enrichedFederationProperties.put("bq.dataset_name", datasourceConfigs.get(SOURCE_DATASET_NAME_KEY_TARGET_FACT));
        enrichedFederationProperties.put("bq.table_name", enrichedTableName);
      } else {
        enrichedTableName = tableName.substring(tableName.indexOf("_") + 1).replaceFirst("\\.", "__").toLowerCase();
        enrichedFederationProperties.put("bq.project_id", datasourceConfigs.get(SOURCE_PROJECT_ID_KEY));
        enrichedFederationProperties.put("bq.dataset_name", datasourceConfigs.get(SOURCE_DATASET_NAME_KEY));
        enrichedFederationProperties.put("bq.table_name", enrichedTableName);
      }

    }
    return enrichedFederationProperties;
  }

  private Map<String, String> modifyFederationProperties(Table table,
      Map<String, String> federationProperties, String storeIdentifier) {
    String tableName = table.getName();
    if (MapUtils.isEmpty(federationProperties)) {
      Map<String, String> enrichedFederationProperties = enrichFederationProperties(tableName, storeIdentifier);
      federationProperties.putAll(enrichedFederationProperties);
    }
    return federationProperties;
  }

  public Map<String, String> getBqProperties(String fullyQualifiedTableName,
      String storeIdentifier) {
    String[] partsFrom = fullyQualifiedTableName.split("\\.");
    String tableName = partsFrom[partsFrom.length - 1];
    Map<String, String> federationPropsForTable = getTableFederation(tableName, storeIdentifier);
    if (MapUtils.isEmpty(federationPropsForTable)) {
      Map<String, String> enrichedFederationProperties = enrichFederationProperties(fullyQualifiedTableName, storeIdentifier);
      federationPropsForTable.putAll(enrichedFederationProperties);
    }
    return federationPropsForTable;
  }

  private Map<String, String> getTableFederation(String tableName, String storeIdenitifier) {
    List<TableFederation> tableFederation = tableFederationDao.getFederation(tableName, storeIdenitifier);

    return Objects.isNull(tableFederation) || tableFederation.size()==0 ? Maps.newHashMap()
        : tableFederation.get(0).getTableProperties();
  }

  public Table getTable(String dataSourceName){
    DataSource dataSource = getDataSourceByName(dataSourceName);
    return DataSourceUtil.getTable(dataSource);
  }



  private DataSource getDataSourceByName(String fromTable) {
    return (DataSource) dataSourceDao.filterOne(new PredicateProvider<DataSource>() {
                                     @Override
                                     protected Predicate _getPredicate(CriteriaBuilder
                                         criteriaBuilder, Root<DataSource> root,
                                         Filter filter) {
                                       Predicate namePredicate = criteriaBuilder.equal(root.get("name")
                                           , fromTable);
                                       Predicate nonDeletedPredicate = criteriaBuilder.equal(root.get
                                           ("deleted"), false);
                                       return criteriaBuilder.and(nonDeletedPredicate, namePredicate);
                                     }
                                   }
        , null);
  }


  public Optional<ReportFederation> getFederationFromTable(String fromTable,
      ReportAction reportAction) {
    List<ReportFederation> reportFederations = reportFederationDao.getFederations(
        StringUtils.EMPTY, fromTable, reportAction);
    return chooseOneFederation(reportFederations);
  }

  private ReportDataResponse executeQuery(QueryRefreshRequest queryRefreshRequest,
      long currentTime) {
    final String userName = ContextProvider.getCurrentSuperBiContext().getUserName();
    final Map<String, String> userSecurityAttributes = gringottsClient.getUserSecurityAttributes(
        userName);

    return executeQuery(queryRefreshRequest, currentTime, userName,
        userSecurityAttributes);
  }

  private ReportDataResponse executeQuery(QueryRefreshRequest queryRefreshRequest,
      long currentTime, final String userName,
      final Map<String, String> userSecurityAttributes) {
    log.info("DataService executeQuery");
//    final Object translatedQuery = backgroundQueryExecutorAdaptor.getTranslatedQuery(
//        storeIdentifier, dsQuery, params);

    //DONE: SSI-1500 do safeMode config per source
    final Boolean isSourceRunningInSafeMode = superbiConfig
        .isSourceRunningInSafeMode(queryRefreshRequest.getStoreIdentifier());

    boolean isCriticalUser = isCriticalUser(userSecurityAttributes, userName);
    if (isSourceRunningInSafeMode && !isCriticalUser) {
      String errorMessage = "Oops!! Currently we are facing heavy load therefore access is "
          + "restricted only for priority users.";
      throw new ServiceUnavailableException(errorMessage);
    }

    final String cacheKey = cacheKeyGenerator.getCacheKey(queryRefreshRequest,
        queryRefreshRequest.getStoreIdentifier());

    final String factName = queryRefreshRequest.getFromTable();

    QueryResultInfo existingQueryResultInfo = getQueryResultInfo(cacheKey,
        queryRefreshRequest.getStoreIdentifier(),
        factName, currentTime);

    return triggerBackgroundRefreshAndSendResponse(queryRefreshRequest, existingQueryResultInfo);
  }

  public QueryResultInfo getQueryResultInfo(String cacheKey, String storeIdentifier,
      String factName, long currentTime) {
    Optional<QueryResultCachedValue> queryCachedResultOptional = resultStore
        .get(cacheKey, QueryResultCachedValue.class);
    Optional<AttemptInfo> attemptInfoOptional = attemptStore
        .get(getAttemptKey(cacheKey), AttemptInfo.class);

    //##############################################################################################

    CacheExpiryConfig cacheExpiryConfig = superbiConfig.getCacheExpiryConfig(storeIdentifier);

    long factRefreshedAtTime = backgroundQueryExecutorAdaptor.getFactRefreshTime(factName,
        storeIdentifier);
    long evictEverythingBefore = cacheExpiryConfig.getEvictEverythingBeforeTimestamp();
    long refreshEverythingBefore = cacheExpiryConfig.getRefreshEverythingBeforeTimestamp();

    QueryResultCachedValue queryCachedResult = null;
    // If query data present
    if (queryCachedResultOptional.isPresent()) {
      log.info("Query result was found in cache for key {}",
          cacheKey);
      queryCachedResult = queryCachedResultOptional.get();
      //Check evict everything before
      if (queryCachedResult.getCachedAtTime() < evictEverythingBefore) {
        //Evict Data.
        queryCachedResult = null;
      }
    }

    AttemptInfo attemptInfo = null;
    // If attempt info was found
    if (attemptInfoOptional.isPresent()) {
      attemptInfo = attemptInfoOptional.get();
      //Check evict everything before
      if (attemptInfo.getCachedAtTime() < evictEverythingBefore) {
        //Evict attempt.
        attemptInfo = null;
      }
    }

    queryCachedResult = makeDataAppropriateForClient(
        ContextProvider.getCurrentSuperBiContext().getClientId(),
        queryCachedResult);

    final BackgroundRefresherConfig backgroundRefresherConfig = superbiConfig
        .getBackgroundRefresherConfig(storeIdentifier);
    //Data is not present or got evicted.
    // If no result then cacheAtTime is 0 else get from result
    long resultCachedAtTime = queryCachedResult != null ? queryCachedResult.getCachedAtTime() : 0;
    long refreshCacheAfter =
        resultCachedAtTime + backgroundRefresherConfig.getRefreshIntervalInSec() * 1000;

    final boolean refreshRequired = isRefreshRequired(currentTime, factRefreshedAtTime,
        refreshEverythingBefore, resultCachedAtTime, refreshCacheAfter);

    final long freshAsOf = getFreshAsOf(currentTime, factRefreshedAtTime, resultCachedAtTime);

    //AttemptInfo is null or ServerSide exception or check if report is not locked due to client side exception.
    final boolean queryLocked = isReportLockedDueToClientSideException(
        backgroundRefresherConfig, attemptInfo,
        currentTime);

    return QueryResultInfo.builder()
        .queryCachedResult(queryCachedResult)
        .attemptInfo(attemptInfo)
        .factRefreshedAtTime(factRefreshedAtTime)
        .cacheKey(cacheKey)
        .refreshRequired(refreshRequired)
        .freshAsOf(freshAsOf)
        .currentTime(currentTime)
        .queryLocked(queryLocked)
        .build();
  }

  private void updateDataStalenessHistogram(final QueryResultCachedValue queryCachedResult,
      final String storeIdentifier, long freshAsOf, long currentTime) {
    if (Objects.nonNull(queryCachedResult) && freshAsOf != 0 && dateStalenessHistograms.containsKey(
        storeIdentifier)) {
      dateStalenessHistograms.get(storeIdentifier).update((currentTime - freshAsOf) / 1000);
    }
  }

  ReportDataResponse triggerBackgroundRefreshAndSendResponse(
      QueryRefreshRequest queryRefreshRequest, QueryResultInfo queryResultInfo) {
    log.info("DataService - triggerBackgroundRefreshAndSendResponse");
    long startTime = System.currentTimeMillis();
    QueryResultCachedValue queryCachedResult = queryResultInfo.getQueryCachedResult();

    //Data is not present or got evicted.
    // If its a DSQueryRefreshRequest, update queryCachedResult with new params
    if (queryCachedResult != null && queryRefreshRequest instanceof DSQueryRefreshRequest) {
      //Wrap with schema.
      DSQueryRefreshRequest dsQueryRefreshRequest = (DSQueryRefreshRequest) queryRefreshRequest;
      queryCachedResult = queryCachedResult
          .copy(queryCachedResult.getValue(),
              QueryUtil.buildSchema(dsQueryRefreshRequest.getDsQuery(),
                  dsQueryRefreshRequest.getParams()));
    }

    updateDataStalenessHistogram(queryCachedResult, queryRefreshRequest.getStoreIdentifier(),
        queryResultInfo.getFreshAsOf(), queryResultInfo.getCurrentTime());

    DATA_CALL_TYPE dataCallType = null;
    //AttemptInfo is null or ServerSide exception or check if query is not locked due to client side exception.
    if (queryResultInfo.isQueryLocked()) {
      log.info("queryResultInfo.isQueryLocked() : " + queryResultInfo.isQueryLocked());
      dataCallType = DATA_CALL_TYPE.QUERY_LOCKED;
    } else if (queryResultInfo.isRefreshRequired()) {
      log.info("queryResultInfo.isRefreshRequired() : " + queryResultInfo.isRefreshRequired());
      dataCallType = queryResultInfo == null ? DATA_CALL_TYPE.POLL : DATA_CALL_TYPE.REFRESH;
      log.info("dataCallType : " + dataCallType);
      try {
        log.info("doing a triggerBackgroundRefresh");
        triggerBackgroundRefresh(queryRefreshRequest, queryResultInfo, dataCallType);
      } catch (MalformedQueryException e){
        throw new MalformedQueryException(e.getMessage());
      } catch (Exception e) {
        throw new SuperBiRuntimeException(e.getMessage(),e , queryCachedResult);
      }
    } else {

      long elapsedTime = System.currentTimeMillis() - startTime;
      log.info("elapsedTime  : " + elapsedTime);
      ExecutionLog.ExecutionLogBuilder executionLogBuilder = ExecutionLog.builder();
      String generatedId = UUID.randomUUID().toString();
      log.info("generatedId : " + generatedId);
      log.info("requestId - queryResultInfo.getAttemptInfo().getRequestId() : " + queryResultInfo.getAttemptInfo().getRequestId());
      executionLogBuilder.requestId(queryResultInfo.getAttemptInfo().getRequestId())
              .id(generatedId)
              .sourceName(queryRefreshRequest.getStoreIdentifier())
              .factName(queryRefreshRequest.getFromTable())
              .message(queryResultInfo.getAttemptInfo().getErrorMessage())
              .totalTimeMs(elapsedTime)
              .startTimeStampMs(startTime)
//              .translatedQuery()
              .isCompleted(true)
              .cacheHit(true);
      executionAuditor.audit(executionLogBuilder.build());
      log.info("DataService - triggerBackgroundRefreshAndSendResponse - executionLogBuilder.cacheHit(true);");
    }

    return ReportDataResponse.builder()
        .dataCallType(dataCallType)
        .appliedFilters(queryRefreshRequest.getAppliedFilters())
        .queryCachedResult(queryCachedResult)
        .attemptInfo(queryResultInfo.getAttemptInfo())
        .freshAsOf(queryResultInfo.getFreshAsOf())
        .cacheKey(queryResultInfo.getCacheKey())
        .build();
  }

  public static long getFreshAsOf(long currentTime, long factRefreshedAtTime,
      long resultCachedAtTime) {
    if (factRefreshedAtTime == 0L || factRefreshedAtTime > resultCachedAtTime) {
      return resultCachedAtTime;
    }
    return currentTime;
  }

  private QueryResultCachedValue makeDataAppropriateForClient(String client,
      QueryResultCachedValue queryCachedResult) {
    if (d42UploadClients.contains(client) && queryCachedResult != null && queryCachedResult
        .isTruncated() &&
        (queryCachedResult.getD42Link() == null || queryCachedResult.getD42Link().isEmpty())) {
      return null;
    }
    return queryCachedResult;
  }

  //Check if report locked due to client side exception.
  private boolean isReportLockedDueToClientSideException(
      BackgroundRefresherConfig backgroundRefresherConfig, AttemptInfo attemptInfo,
      long currentTime) {
    if (attemptInfo == null) {
      return false;
    }
    if (attemptInfo.isSuccessful()) {
      return false;
    }
    if (attemptInfo.isServerError()) {
      return false;
    }
    //It must be a failed attempt with client side error
    long lockUntil = attemptInfo.getCachedAtTime() +
        (backgroundRefresherConfig.getAttemptInfoTtlInSecForClientSideException() * 1000);
    return lockUntil > currentTime;
  }

  //Check if refresh is required due to softExpiry or forced refresh due to config.
  // Or due to underlying fact refresh.
  private static boolean isRefreshRequired(long currentTime, long factRefreshedAtTime,
      long refreshEverythingBefore, long resultCachedAtTime, long refreshCacheAfter) {
    return refreshCacheAfter < currentTime || resultCachedAtTime < refreshEverythingBefore
        || factRefreshedAtTime > resultCachedAtTime;
  }

  //################################################################################################

  private boolean isCriticalUser(Map<String, String> securityAttributes, String userName) {
    try {
      return Boolean.parseBoolean(securityAttributes.get("critical"));
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  private String enrichStoreIdentifier(Source source,Optional<String> executionEngine) {
    String enrichedSourceName;
    switch (source.getSourceType()) {
      case VERTICA:
        if("Target".equals(source.getName())){
          enrichedSourceName = superbiConfig.getStoreIdentifiersForTableEnrich().get(TARGET_FACT_OVERRIDE_KEY).get(0);
          break;
        }
        enrichedSourceName = superbiConfig.getStoreIdentifiersForTableEnrich().get(BATCH_OVERRIDE_KEY).get(0);
        break;
      case ELASTIC_SEARCH:
        enrichedSourceName = superbiConfig.getStoreIdentifiersForTableEnrich().get(REALTIME_OVERRIDE_KEY).get(0);
        break;
      case HDFS:
        if (executionEngine != null && executionEngine.isPresent() && executionEngine.get()
            .equals(BIG_QUERY_EXECUTION_ENGINE)) {
          enrichedSourceName = superbiConfig.getStoreIdentifiersForTableEnrich()
              .get(BATCH_HDFS_OVERRIDE_KEY).get(0);
        } else {
          enrichedSourceName = String.format("%s_%s", source.getName(), DEFAULT);
        }
        break;
      default:
        enrichedSourceName = String.format("%s_%s", source.getName(), DEFAULT);
    }
    return enrichedSourceName;
  }

  @NotNull
  public String getStoreIdentifierForFact(String fromTable,
      Optional<ReportFederation> reportFederation,
      Optional<String> executionEngine) {
    Fact fact = getFactByName(fromTable);
    Source source = fact.getTable().getSource();
    return getStoreIdentifier(reportFederation, executionEngine, source);
  }

  @NotNull
  public String getStoreIdentifierForDimension(String dimension,
      Optional<ReportFederation> reportFederation,
      Optional<String> executionEngine) {
    Dimension dim = getDimensionByName(dimension);
    Source source = dim.getTable().getSource();
    return getStoreIdentifier(reportFederation,executionEngine,source);
  }

  private String getStoreIdentifier(Optional<ReportFederation> reportFederation,
      Optional<String> executionEngine, Source source) {
    if (!executionEngine.isPresent() || executionEngine.get().equals(EMPTY_STRING)) {
      if (source.getSourceType().equals(SourceType.HDFS)) {
        return enrichStoreIdentifier(source, executionEngine);
      } else {
        return getStoreIdentifier(source, reportFederation);
      }
    }
    switch (executionEngine.get()) {
      case BIG_QUERY_EXECUTION_ENGINE:
        if (reportFederation.isPresent() && StringUtils.isNotBlank(
            reportFederation.get().getOverriding_store_identifier())) {
          return reportFederation.get().getOverriding_store_identifier();
        }
        // Store identifier will either be from Overriding_store_identifier or DEFAULT
        return enrichStoreIdentifier(source, executionEngine);
      case HDFS_EXECUTION_ENGINE:
      default:
        return enrichStoreIdentifier(source, executionEngine);
    }
  }

  @NotNull
  public String getStoreIdentifier(Source source, Optional<ReportFederation> reportFederation) {
    /**
     * TODO verify if we can use?
     * //    reportSTO.getQueryFormMetaDetailsMap().getContext().schemaStore.getFact("")
     * .getTableSource();
     */
    if (reportFederation.isPresent() && StringUtils.isNotBlank(
        reportFederation.get().getOverriding_store_identifier())) {
      return reportFederation.get().getOverriding_store_identifier();
    }
    // Store identifier will either be from Overriding_store_identifier or DEFAULT
    return enrichStoreIdentifier(source,Optional.empty());
  }

  @NotNull
  public Source getSourceByFactName(String factName) {
    Fact fact = getFactByName(factName);
    return fact.getTable().getSource();
  }

  private Fact getFactByName(String fromTable) {
    return factDao.filterOne(new PredicateProvider<Fact>() {
                               @Override
                               protected Predicate _getPredicate(CriteriaBuilder
                                   criteriaBuilder, Root<Fact> root,
                                   Filter filter) {
                                 Predicate namePredicate = criteriaBuilder.equal(root.get("name")
                                     , fromTable);
                                 Predicate nonDeletedPredicate = criteriaBuilder.equal(root.get
                                     ("deleted"), false);
                                 return criteriaBuilder.and(nonDeletedPredicate, namePredicate);
                               }
                             }
        , null);
  }

  private Dimension getDimensionByName(String dimension) {
    return dimensionDao.filterOne(new PredicateProvider<Dimension>() {
                                    @Override
                                    protected Predicate _getPredicate(CriteriaBuilder
                                        criteriaBuilder, Root<Dimension> root,
                                        Filter filter) {
                                      Predicate namePredicate = criteriaBuilder.equal(root.get("name")
                                          , dimension);
                                      Predicate nonDeletedPredicate = criteriaBuilder.equal(root.get
                                          ("deleted"), false);
                                      return criteriaBuilder.and(nonDeletedPredicate, namePredicate);
                                    }
                                  }
        , null);
  }

  @NotNull
  public DSQueryBuilder.QueryAndParam getQueryAndParam(QueryPanel queryPanel,
      Map<String, String[]> params, DataPrivilege dataPrivilege, Integer sourceLimit) {
    // fetch report from dao

    if (queryPanel.getMissingColumnExceptions().size() > 0) {
      log.error("Missing column found");
      System.out.println(queryPanel.getMissingColumns());
      List<String> missingColumnNames = queryPanel
          .getMissingColumns()
          .stream().map(mc -> mc.getColumn().name).collect(Collectors.toList());

      throw new MissingColumnsException(queryPanel.getFromTable(),
          missingColumnNames);
    }

    com.google.common.base.Optional<Integer> limitFromParams = QueryUtil.extractLimit(params);

    // generate DSQuery
    DSQueryBuilder dsQueryBuilder = DSQueryBuilder
        .getFor(queryPanel, limitFromParams, dataPrivilege, sourceLimit, nativeExpressionDao);

    com.google.common.base.Optional<DSQueryBuilder.QueryAndParam> queryAndParamOp =
        dsQueryBuilder.getQueryAndParam();

    if (!queryAndParamOp.isPresent()) {
      throw new MalformedReportException("Report query could not be formed");
    }
    return queryAndParamOp.get();
  }

  private void triggerBackgroundRefresh(final QueryRefreshRequest queryRefreshRequest,
      final QueryResultInfo queryResultInfo,
      DATA_CALL_TYPE dataCallType) {
    log.info("triggerBackgroundRefresh started..");
    /**
     * Set the deadline for the query to be able to pick in the predefied milliseconds, Post
     * which it can be discarded from submitting to sink
     * set deadline, queryWeight here
     */
    AdaptorQueryPayload.AdaptorQueryPayloadBuilder adaptorQueryPayloadBuilder = AdaptorQueryPayload.builder();

    adaptorQueryPayloadBuilder
        .cacheKey(queryResultInfo.getCacheKey())
        .attemptKey(getAttemptKey(queryResultInfo.getCacheKey()))
        .storeIdentifier(queryRefreshRequest.getStoreIdentifier())
        .requestId(MDC.get(REQUEST_ID))
        .reportAction(
            ContextProvider.getCurrentSuperBiContext().getClientPrivilege().getReportAction()
                .name());

    if (queryRefreshRequest instanceof DSQueryRefreshRequest) {
      log.info("triggerBackgroundRefresh - queryRefreshRequest instanceof DSQueryRefreshRequest");
      DSQueryRefreshRequest dsQueryRefreshRequest = (DSQueryRefreshRequest) queryRefreshRequest;
      DataSource dataSource = getDataSourceByName(dsQueryRefreshRequest.getFromTable());
      Table table = DataSourceUtil.getTable(dataSource);

      NativeQuery nativeQuery = backgroundQueryExecutorAdaptor.getTranslatedQuery(
          queryRefreshRequest.getStoreIdentifier(),
          dsQueryRefreshRequest.getDsQuery(), dsQueryRefreshRequest.getParams(),
          modifyFederationProperties(table, dsQueryRefreshRequest.getFederationProperties(),
              queryRefreshRequest.getStoreIdentifier()),
          (tableName) -> getBqProperties(tableName,
              dsQueryRefreshRequest.getStoreIdentifier()),
          (fromTable) -> getDataSourceByName(fromTable), getRulesForEuclidFact(
              dsQueryRefreshRequest.getFromTable()));


      adaptorQueryPayloadBuilder
          .dsQuery(dsQueryRefreshRequest.getDsQuery())
          .dateRange(dsQueryRefreshRequest.getDateRange())
          .params(dsQueryRefreshRequest.getParams())
          .nativeQuery(nativeQuery);

    } else if (queryRefreshRequest instanceof NativeQueryRefreshRequest) {
      log.info("triggerBackgroundRefresh - queryRefreshRequest instanceof NativeQueryRefreshRequest");
      adaptorQueryPayloadBuilder
          .nativeQuery(((NativeQueryRefreshRequest) queryRefreshRequest).getNativeQuery());
    }

    if(queryRefreshRequest instanceof DSQueryRefreshRequest && ((DSQueryRefreshRequest) queryRefreshRequest).getReportName().isPresent()) {
      adaptorQueryPayloadBuilder.metaDataPayload(MetaDataPayload.builder()
              .username(ContextProvider.getCurrentSuperBiContext().getUserName())
              .client(ContextProvider.getCurrentSuperBiContext().getClientId())
              .reportName(((DSQueryRefreshRequest) queryRefreshRequest).getReportName().get())
              .factName(queryRefreshRequest.getFromTable()).build());
    } else if (queryRefreshRequest instanceof NativeQueryRefreshRequest && ((NativeQueryRefreshRequest) queryRefreshRequest).getExecutionEngineLabels() != null) {
      adaptorQueryPayloadBuilder.metaDataPayload(MetaDataPayload.builder()
              .username(ContextProvider.getCurrentSuperBiContext().getUserName())
              .client(ContextProvider.getCurrentSuperBiContext().getClientId())
              .reportName(null)
              .executionEngineLabels(((NativeQueryRefreshRequest) queryRefreshRequest).getExecutionEngineLabels())
              .factName(queryRefreshRequest.getFromTable()).build());
    }
    else {
      adaptorQueryPayloadBuilder.metaDataPayload(MetaDataPayload.builder()
              .username(ContextProvider.getCurrentSuperBiContext().getUserName())
              .client(ContextProvider.getCurrentSuperBiContext().getClientId())
              .reportName(null)
              .factName(queryRefreshRequest.getFromTable()).build());
    }

    AdaptorQueryPayload adaptorQueryPayload = adaptorQueryPayloadBuilder.build();
    backgroundQueryExecutorAdaptor.submitQuery(adaptorQueryPayload);

    log.info("Triggering background refresh <{}> with cacheKey = {}",
        dataCallType.name(), queryResultInfo.getCacheKey());

    QueryInfo queryInfo = QueryInfo.builder()
        .reportOrg("NA")
        .reportNamespace("NA")
        .reportName("NA")
        .requestId(MDC.get(REQUEST_ID)).cacheKey(adaptorQueryPayload.getCacheKey())
        .dataCallType(dataCallType).build();

    auditer.audit(queryInfo);
  }

  private String getAttemptKey(String cacheKey) {
    return "attempt_key_" + cacheKey;
  }

  @Timed
  public Optional<ReportFederation> getFederationForReport(ReportSTO reportSTO,
      ReportAction reportAction) {
    Preconditions.checkNotNull(reportAction);
    Preconditions.checkNotNull(reportSTO);

    String fromTable = reportSTO.getQueryFormMetaDetailsMap().getFromTable();

    List<ReportFederation> reportFederations = reportFederationDao.getFederations(
        reportSTO.getQualifiedReportName(), fromTable, reportAction);
    return chooseOneFederation(reportFederations);
  }

  @Timed
  public List<String> getRulesForEuclidFact(String factName) {
    Preconditions.checkNotNull(factName);
    List<EuclidRules> euclidRules = euclidRulesDao.getFactRules(
        factName);
    if (Objects.isNull(euclidRules) || euclidRules.isEmpty()){
      return Lists.newArrayList();
    }
    return euclidRules.stream()
          .map(EuclidRules::getRuleDefinition)
          .collect(Collectors.toList());
  }

  private Optional<ReportFederation> chooseOneFederation(List<ReportFederation> reportFederations) {
    if (CollectionUtils.isNotEmpty(reportFederations)) {
      reportFederations.sort(reportActionFederationPolicy);
      return Optional.ofNullable(reportFederations.get(0));
    }
    return Optional.empty();
  }

  private Map<String, Histogram> registerDataStalenessHistogramsForSources() {
    if (CollectionUtils.isEmpty(superbiConfig.getDataSourcesList())) {
      return Collections.emptyMap();
    }
    return superbiConfig.getDataSourcesList().stream().collect(Collectors.toMap(Function.identity(),
        source -> metricRegistry.histogram(
            MetricRegistry.name(DATA_STALENESS_METRIC_KEY, source))));
  }

  public Statistics getHibernateStatistics() {
    SessionFactory sessionFactory = dataSourceDao.getEntityManager().getEntityManagerFactory().unwrap(SessionFactory.class);
    return sessionFactory.getStatistics();
  }

  private boolean validatePartitionKey(Fact fact, List<PanelEntry> validationColumns) {
    try {
      BatchCubeGuardrailConfig batchCubeGuardrailConfig = superbiConfig.getBatchCubeGuardrailConfig();
      if (batchCubeGuardrailConfig.getBatchCubesFactList().contains(fact.getName())) {
        boolean enablePartitionKeyValidation = batchCubeGuardrailConfig.isGlobalEnablePartitionKeyValidation();
        if (!enablePartitionKeyValidation) {
          return true;
        }

        List<BadgerProcessData> processDataList = backgroundQueryExecutorAdaptor.getAllActiveProcessDatas(
            fact.getNamespace().getOrg().getName(), fact.getNamespace().getName(), fact.getName());
        BadgerProcessData processData = processDataList.stream()
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);

        if (processData == null) {
          return true;
        }

        List<BadgerEntityColumn> columns = processData.getEntities().get(0).getColumns();
        for (PanelEntry column : validationColumns) {
          String columnKey = column.getColumn().getColumnName();
          Optional<BadgerEntityColumn> matchingColumn = columns.stream()
              .filter(col -> col.getName().equals(columnKey))
              .findFirst();
          if (matchingColumn.isPresent()) {
            boolean isPartitionedColumn = matchingColumn.get().isPartitioned();
            if (isPartitionedColumn) {
              return true;
            }
          }
        }
        return false;
      }
      return true;
    } catch (Exception e) {
      log.error("Exception occurred during partition key validation", e);
      return true;
    }
  }

}
