package com.flipkart.fdp.superbi.core.service;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.mmg.cosmos.dao.DataSourceDao;
import com.flipkart.fdp.mmg.cosmos.dao.DimensionDao;
import com.flipkart.fdp.mmg.cosmos.dao.FactDao;
import com.flipkart.fdp.mmg.cosmos.entities.Fact;
import com.flipkart.fdp.mmg.cosmos.entities.Source;
import com.flipkart.fdp.mmg.cosmos.entities.SourceType;
import com.flipkart.fdp.mmg.cosmos.entities.Table;
import com.flipkart.fdp.superbi.core.adaptor.BackgroundQueryExecutorAdaptor;
import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.core.cache.CacheKeyGenerator;
import com.flipkart.fdp.superbi.core.config.CacheExpiryConfig;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.core.config.DataPrivilege;
import com.flipkart.fdp.superbi.core.config.DataPrivilege.LimitPriority;
import com.flipkart.fdp.superbi.core.config.SuperbiConfig;
import com.flipkart.fdp.superbi.core.context.ContextProvider;
import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.model.QueryInfo.DATA_CALL_TYPE;
import com.flipkart.fdp.superbi.core.model.QueryRefreshRequest;
import com.flipkart.fdp.superbi.core.model.QueryResultInfo;
import com.flipkart.fdp.superbi.core.model.ReportDataResponse;
import com.flipkart.fdp.superbi.dao.EuclidRulesDao;
import com.flipkart.fdp.superbi.dao.NativeExpressionDao;
import com.flipkart.fdp.superbi.dao.ReportFederationDao;
import com.flipkart.fdp.superbi.dao.TableFederationDao;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.entities.ReportAction;
import com.flipkart.fdp.superbi.entities.ReportFederation;
import com.flipkart.fdp.superbi.entities.TableFederation;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static com.flipkart.fdp.superbi.cosmos.utils.Constants.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@Slf4j
public class DataServiceTest {

  public static final String CACHE_KEY = "CACHE_KEY";
  public static final String STORE_IDENTIFIER = "STORE_IDENTIFIER";
  public static final String FACT_NAME = "FACT_NAME";
  public static final long TIME_1K = 1000;
  public static final long TIME_5K = 5 * TIME_1K;
  public static final long TIME_1000K = 1000 * TIME_1K;

  private DataService dataService;

  final BackgroundQueryExecutorAdaptor backgroundQueryExecutorAdaptor = Mockito.mock(
      BackgroundQueryExecutorAdaptor.class);
  @Mock
  private CacheKeyGenerator cacheKeyGenerator;
  @Mock
  private CacheDao resultStore;
  @Mock
  private CacheDao attemptStore;
  @Mock
  private FactDao factDAO;
  @Mock
  private GringottsClient gringottsClient;
  @Mock
  private SuperbiConfig superbiConfig;
  @Mock
  private ReportFederationDao reportFederationDao;
  @Mock
  private MetricRegistry metricRegistry;
  @Mock
  private Auditer auditer;
  @Mock
  private DimensionDao dimensionDao;
  @Mock
  private EuclidRulesDao euclidRulesDao;
  @Mock
  private DataSourceDao dataSourceDao;
  @Mock
  private TableFederationDao tableFederationDao;
  @Mock
  private NativeExpressionDao nativeExpressionDao;

  private ExecutionAuditor executionAuditor;
  private TableFederation tableFederationWithDatasetName;
  private TableFederation tableFederationWithTableName;
  private TableFederation tableFederationWithNoProps;
  private TableFederation tableFederationWithAllProps;

  private ReportFederation reportFederationWithStoreIdentifier;
  private ReportFederation reportFederationWithNoStoreIdentifier;
  private ReportFederation exactReportNameExactActionName;
  private ReportFederation exactReportNameExactActionNameWithStoreIdentifier;
  private ReportFederation reportFederationWithWildCardNS;
  private ReportFederation reportFederationWithWildCardOrg;
  private Map<String,String> federationProperties = new HashMap<>();

  @Before
  public void setUp() {
    ClientPrivilege clientPrivilege = new ClientPrivilege(ReportAction.ALL,
        new DataPrivilege(100000, LimitPriority.REPORT, false));
    ContextProvider.setCurrentSuperBiContext("TEST_CLIENT", "TEST_USER", clientPrivilege);
    Mockito.when(backgroundQueryExecutorAdaptor.getFactRefreshTime(Mockito.any(), Mockito.any()))
        .thenReturn(TIME_1K - 500);
    createReportFederations();
    createTableFederation();
    MockitoAnnotations.initMocks(this);
    dataService = new DataService(backgroundQueryExecutorAdaptor,cacheKeyGenerator,resultStore,
        attemptStore,factDAO,gringottsClient,superbiConfig,auditer,reportFederationDao,
        metricRegistry, Collections.emptyMap(),dimensionDao, euclidRulesDao, Collections.emptyList(),
        dataSourceDao, tableFederationDao, nativeExpressionDao, executionAuditor);
  }
  public static final String FULLY_QUALIFIED_TABLE_NAME = "scp_product.temp_fact";



  private DataService createScenario(long evictEverythingBeforeTimestamp, int refreshIntervalInSec,
      Optional<QueryResultCachedValue> queryResultCachedValueOptional,
      Optional<AttemptInfo> attemptInfoOptional) {
    SuperbiConfig superbiConfig = Mockito.mock(SuperbiConfig.class);
    CacheExpiryConfig cacheExpiryConfig = Mockito.mock(CacheExpiryConfig.class);
    BackgroundRefresherConfig backgroundRefresherConfig = Mockito.mock(
        BackgroundRefresherConfig.class);
    Mockito.when(superbiConfig.getCacheExpiryConfig(Mockito.anyString()))
        .thenReturn(cacheExpiryConfig);
    Mockito.when(superbiConfig.getBackgroundRefresherConfig(Mockito.anyString()))
        .thenReturn(backgroundRefresherConfig);

    CacheDao resultStore = Mockito.mock(CacheDao.class);
    CacheDao attemptStore = Mockito.mock(CacheDao.class);
    FactDao factDao = Mockito.mock(FactDao.class);
    DataSourceDao dataSourceDao = Mockito.mock(DataSourceDao.class);
    TableFederationDao tableFederationDao = Mockito.mock(TableFederationDao.class);
    NativeExpressionDao nativeExpressionDao = Mockito.mock(NativeExpressionDao.class);
    // Force evict cache before `evictEverythingBeforeTimestamp`
    Mockito.when(cacheExpiryConfig.getEvictEverythingBeforeTimestamp())
        .thenReturn(evictEverythingBeforeTimestamp);

    // Refresh data after `refreshIntervalInSec`
    Mockito.when(backgroundRefresherConfig.getRefreshIntervalInSec())
        .thenReturn(refreshIntervalInSec);

    Mockito.when(
            resultStore.get(Mockito.anyString(), Matchers.<Class<QueryResultCachedValue>>any()))
        .thenReturn(
            queryResultCachedValueOptional);
    Mockito.when(attemptStore.get(Mockito.anyString(), Matchers.<Class<AttemptInfo>>any()))
        .thenReturn(
            attemptInfoOptional);

    return new DataService(backgroundQueryExecutorAdaptor,
        null, resultStore, attemptStore,
        factDao, null, superbiConfig,
        Mockito.mock(Auditer.class), null, null, null,
        null, euclidRulesDao, Lists.newArrayList(), dataSourceDao, tableFederationDao, nativeExpressionDao, executionAuditor);
  }

  @Test
  public void testQueryResultInfoWhenNoDataInCache() {
    DataService dataService = createScenario(0, 0, Optional.empty(), Optional.empty());

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_1K);

    assertRefreshRequiredWithNoDataToReturn(queryResultInfo, 0);
  }

  @Test
  public void testQueryResultInfoWhenEvictedDataInCache() {
    // Data in Cache and AttemptInfo is set to be force evicted
    QueryResultCachedValue evictedData = Mockito.mock(QueryResultCachedValue.class);
    Mockito.when(evictedData.getCachedAtTime()).thenReturn(TIME_1K);

    AttemptInfo attemptInfo = Mockito.mock(AttemptInfo.class);
    Mockito.when(attemptInfo.getCachedAtTime()).thenReturn(TIME_1K);

    // Force evict cached before 5K
    DataService dataService = createScenario(TIME_5K, 0, Optional.of(evictedData),
        Optional.of(attemptInfo));

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_1000K);

    assertRefreshRequiredWithNoDataToReturn(queryResultInfo, 0);
  }

  @Test
  public void testQueryResultInfoWhenStaleDataInCache() {
    QueryResultCachedValue evictedData = Mockito.mock(QueryResultCachedValue.class);
    Mockito.when(evictedData.getCachedAtTime()).thenReturn(TIME_1K);

    AttemptInfo attemptInfo = Mockito.mock(AttemptInfo.class);
    Mockito.when(attemptInfo.getCachedAtTime()).thenReturn(TIME_1K);

    // Refresh data after 2 seconds
    DataService dataService = createScenario(0, 2, Optional.of(evictedData),
        Optional.of(attemptInfo));

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_5K);

    assertRefreshRequiredWithStaleDataToReturn(queryResultInfo, TIME_5K);
  }

  @Test
  public void testQueryResultInfoWhenDataInCacheIsFresh() {
    QueryResultCachedValue evictedData = Mockito.mock(QueryResultCachedValue.class);
    Mockito.when(evictedData.getCachedAtTime()).thenReturn(TIME_1K);

    AttemptInfo attemptInfo = Mockito.mock(AttemptInfo.class);
    Mockito.when(attemptInfo.getCachedAtTime()).thenReturn(TIME_1K);

    // Refresh data after 1000 seconds
    DataService dataService = createScenario(0, 1000, Optional.of(evictedData),
        Optional.of(attemptInfo));

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_5K);

    assertNoRefreshRequiredWithFreshDataToReturn(queryResultInfo, TIME_5K);
  }

  private void assertRefreshRequiredWithNoDataToReturn(QueryResultInfo queryResultInfo,
      long freshAsOf) {
    Assert.assertNull(queryResultInfo.getQueryCachedResult());
    Assert.assertNull(queryResultInfo.getAttemptInfo());
    Assert.assertTrue(queryResultInfo.isRefreshRequired());
    Assert.assertFalse(queryResultInfo.isQueryLocked());
    Assert.assertEquals(queryResultInfo.getCacheKey(), CACHE_KEY);
    Assert.assertEquals(queryResultInfo.getFreshAsOf(), freshAsOf);
  }

  private void assertRefreshRequiredWithStaleDataToReturn(QueryResultInfo queryResultInfo,
      long freshAsOf) {
    Assert.assertNotNull(queryResultInfo.getQueryCachedResult());
    Assert.assertNotNull(queryResultInfo.getAttemptInfo());
    Assert.assertTrue(queryResultInfo.isRefreshRequired());
    Assert.assertFalse(queryResultInfo.isQueryLocked());
    Assert.assertEquals(queryResultInfo.getCacheKey(), CACHE_KEY);
    Assert.assertEquals(queryResultInfo.getFreshAsOf(), freshAsOf);
  }

  private void assertNoRefreshRequiredWithFreshDataToReturn(QueryResultInfo queryResultInfo,
      long freshAsOf) {
    Assert.assertNotNull(queryResultInfo.getQueryCachedResult());
    Assert.assertNotNull(queryResultInfo.getAttemptInfo());
    Assert.assertFalse(queryResultInfo.isRefreshRequired());
    Assert.assertFalse(queryResultInfo.isQueryLocked());
    Assert.assertEquals(queryResultInfo.getCacheKey(), CACHE_KEY);
    Assert.assertEquals(queryResultInfo.getFreshAsOf(), freshAsOf);
  }

  @Test
  public void triggerRefreshWhenNoDataInCacheAndNoDataSentToClient() {
    DataService dataService = createScenario(0, 0, Optional.empty(), Optional.empty());

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_1K);

    QueryRefreshRequest queryRefreshRequest = Mockito.mock(QueryRefreshRequest.class);
    ReportDataResponse reportDataResponse = dataService.triggerBackgroundRefreshAndSendResponse(
        queryRefreshRequest, queryResultInfo);

    verify(backgroundQueryExecutorAdaptor, times(1)).submitQuery(any());
    Assert.assertNull(reportDataResponse.getQueryCachedResult());
    Assert.assertNull(reportDataResponse.getAttemptInfo());

    // Its a POLL request
    Assert.assertEquals(reportDataResponse.getDataCallType(), DATA_CALL_TYPE.POLL);
    Assert.assertEquals(reportDataResponse.getFreshAsOf(), 0);
  }

  @Test
  public void triggerRefreshWhenEvictedDataInCacheAndNoDataSentToClient() {
    // Data in Cache and AttemptInfo is set to be force evicted
    QueryResultCachedValue evictedData = Mockito.mock(QueryResultCachedValue.class);
    Mockito.when(evictedData.getCachedAtTime()).thenReturn(TIME_1K);

    AttemptInfo attemptInfo = Mockito.mock(AttemptInfo.class);
    Mockito.when(attemptInfo.getCachedAtTime()).thenReturn(TIME_1K);

    // Force evict cached before 5K
    DataService dataService = createScenario(TIME_5K, 0, Optional.of(evictedData),
        Optional.of(attemptInfo));

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_1000K);

    QueryRefreshRequest queryRefreshRequest = Mockito.mock(QueryRefreshRequest.class);
    ReportDataResponse reportDataResponse = dataService.triggerBackgroundRefreshAndSendResponse(
        queryRefreshRequest, queryResultInfo);

    verify(backgroundQueryExecutorAdaptor, times(1)).submitQuery(any());
    Assert.assertNull(reportDataResponse.getQueryCachedResult());
    Assert.assertNull(reportDataResponse.getAttemptInfo());

    // Its a POLL request as the data was force evicted
    Assert.assertEquals(reportDataResponse.getDataCallType(), DATA_CALL_TYPE.POLL);
    Assert.assertEquals(reportDataResponse.getFreshAsOf(), 0);
  }

  @Test
  @Ignore
  // TODO
  // This test should pass but code tech debt on(QueryUtil) is failing this one, should fix this later
  public void triggerRefreshWithStaleDataSentToClient() {
    QueryResultCachedValue evictedData = Mockito.mock(QueryResultCachedValue.class);
    Mockito.when(evictedData.getCachedAtTime()).thenReturn(TIME_1K);

    AttemptInfo attemptInfo = Mockito.mock(AttemptInfo.class);
    Mockito.when(attemptInfo.getCachedAtTime()).thenReturn(TIME_1K);

    // Refresh data after 2 seconds
    DataService dataService = createScenario(0, 2, Optional.of(evictedData),
        Optional.of(attemptInfo));

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_5K);

    QueryRefreshRequest queryRefreshRequest = Mockito.mock(QueryRefreshRequest.class);

    ReportDataResponse reportDataResponse = dataService.triggerBackgroundRefreshAndSendResponse(
        queryRefreshRequest, queryResultInfo);

    verify(backgroundQueryExecutorAdaptor, times(1)).submitQuery(any());

    // Send stale data to client
    Assert.assertNotNull(reportDataResponse.getQueryCachedResult());
    Assert.assertNotNull(reportDataResponse.getAttemptInfo());

    // Its a REFRESH request as the client needs to come back for fresh data
    Assert.assertEquals(reportDataResponse.getDataCallType(), DATA_CALL_TYPE.REFRESH);
    Assert.assertEquals(reportDataResponse.getFreshAsOf(), TIME_1K);
  }

  @Test
  @Ignore
  // TODO
  // This test should pass but code tech debt on(QueryUtil) is failing this one, should fix this later
  public void doNotTriggerRefreshWithFreshDataSentToClient() {
    QueryResultCachedValue evictedData = Mockito.mock(QueryResultCachedValue.class);
    Mockito.when(evictedData.getCachedAtTime()).thenReturn(TIME_1K);

    AttemptInfo attemptInfo = Mockito.mock(AttemptInfo.class);
    Mockito.when(attemptInfo.getCachedAtTime()).thenReturn(TIME_1K);

    // Refresh data after 1000 seconds
    DataService dataService = createScenario(0, 1000, Optional.of(evictedData),
        Optional.of(attemptInfo));

    QueryResultInfo queryResultInfo = dataService.getQueryResultInfo(CACHE_KEY,
        STORE_IDENTIFIER, FACT_NAME, TIME_5K);

    QueryRefreshRequest queryRefreshRequest = Mockito.mock(QueryRefreshRequest.class);

    ReportDataResponse reportDataResponse = dataService.triggerBackgroundRefreshAndSendResponse(
        queryRefreshRequest, queryResultInfo);

    // Do not trigger any refresh
    verify(backgroundQueryExecutorAdaptor, times(0)).submitQuery(any());

    // Send stale data to client
    Assert.assertNotNull(reportDataResponse.getQueryCachedResult());
    Assert.assertNotNull(reportDataResponse.getAttemptInfo());

    // Its a terminal request as the data was fresh and client need not to come back
    Assert.assertNull(reportDataResponse.getDataCallType());
    Assert.assertEquals(reportDataResponse.getFreshAsOf(), TIME_5K);
  }

    @Test
    public void testGetFederationFromTable() {
        when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(reportFederationWithStoreIdentifier,reportFederationWithNoStoreIdentifier));
        Optional<ReportFederation> federation = dataService.getFederationFromTable("test_fact", ReportAction.ALL);
        Assert.assertNotNull(federation.get());
        Assert.assertEquals(federationProperties,federation.get().getFederationProperties());
    }

  @Test
  public void testGetFederationFromTable2() {
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(exactReportNameExactActionNameWithStoreIdentifier,reportFederationWithNoStoreIdentifier));
    Optional<ReportFederation> federation = dataService.getFederationFromTable("test_fact", ReportAction.ALL);
    Assert.assertEquals(Maps.newHashMap(),federation.get().getFederationProperties());
  }

    @Test
    public void testDefaultStoreIdentifier() {
        Fact fact = createFact(Optional.empty());
        when(factDAO.filterOne(any(),any())).thenReturn(fact);
        String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact", Optional.empty(),Optional.empty());
        Assert.assertEquals("test_source_DEFAULT",storeIdentifier);
    }

    @Test
    public void testStoreIdentifierFromFederation() {
        Fact fact = createFact(Optional.of(SourceType.VERTICA));
        when(factDAO.filterOne(any(),any())).thenReturn(fact);
        String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact", Optional.of(exactReportNameExactActionNameWithStoreIdentifier),Optional.empty());
        Assert.assertEquals("test_source_P0",storeIdentifier);
    }

    @Test
    public void testOverriddenStoreIdentifier() {
        Fact fact = createFact(Optional.of(SourceType.VERTICA));
        when(factDAO.filterOne(any(),any())).thenReturn(fact);
        String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact", Optional.of(reportFederationWithStoreIdentifier),Optional.empty());
        Assert.assertEquals("store_identifier",storeIdentifier);
    }

    @Test
    public void testNotNullFederationForReport() {
        ReportSTO reportSTO = ReportSTO.builder()
                .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
                .org("org")
                .namespace("ns").build();
        when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(reportFederationWithStoreIdentifier,reportFederationWithNoStoreIdentifier));
        Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
        Assert.assertNotNull(federationForReport.get());
    }

    /*
    reportFederationWithStoreIdentifier is choosen due to exact report name matching
     */
    @Test
    public void testFederationReportWithStoreIdentifier() {
        ReportSTO reportSTO = ReportSTO.builder()
                .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
                .org("org")
                .namespace("ns").build();
        when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(reportFederationWithStoreIdentifier,reportFederationWithNoStoreIdentifier));
        Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
        Assert.assertEquals("store_identifier",federationForReport.get().getOverriding_store_identifier());
        Assert.assertEquals(federationProperties,federationForReport.get().getFederationProperties());
    }

    @Test
    public void testFederationReportWithNoStoreIdentifier() {
        ReportSTO reportSTO = ReportSTO.builder()
                .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
                .org("org")
                .namespace("ns").build();
        when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(reportFederationWithStoreIdentifier,
                reportFederationWithNoStoreIdentifier, exactReportNameExactActionName));
        Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
        Assert.assertNull(federationForReport.get().getOverriding_store_identifier());
    }


  @Test
  public void testFederationReportWithNoDataSourceAndStoreIdentifierPresentForHDFSFact() {
    Fact fact = createFact(Optional.empty());
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.empty()).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithStoreIdentifier));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("test_source_DEFAULT",storeIdentifier);
  }

  @Test
  public void testFederationReportWithNoDataSourceAndStoreIdentifierPresentForOtherThanHDFSFact() {
    Fact fact = createFact(Optional.of(SourceType.VERTICA));
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.empty()).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithStoreIdentifier));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("store_identifier",storeIdentifier);
  }

  @Test
  public void testFederationReportWithNoDataSourceAndNoStoreIdentifier() {
    Fact fact = createFact(Optional.empty());
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.empty()).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithNoStoreIdentifier));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("test_source_DEFAULT",storeIdentifier);
  }

  @Test
  public void testFederationReportWithDataSourceAsHIVEAndStoreIdentifierPresent() {
    // check both QueryPanel and ReportSTO
    Fact fact = createFact(Optional.empty());
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.of(HDFS_EXECUTION_ENGINE)).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .executionEngine(Optional.of(HDFS_EXECUTION_ENGINE))
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithStoreIdentifier));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("test_source_DEFAULT",storeIdentifier);
  }

  @Test
  public void testFederationReportWithDataSourceAsHIVEAndNoStoreIdentifier() {
    // check both QueryPanel and ReportSTO
    Fact fact = createFact(Optional.empty());
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.of(HDFS_EXECUTION_ENGINE)).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .executionEngine(Optional.of(HDFS_EXECUTION_ENGINE))
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithNoStoreIdentifier));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("test_source_DEFAULT",storeIdentifier);
  }

  @Test
  public void testFederationReportWithDataSourceAsBIG_QUERYAndNoStoreIdenfier() {
    Fact fact = createFact(Optional.empty());
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.of(BIG_QUERY_EXECUTION_ENGINE)).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .executionEngine(Optional.of(BIG_QUERY_EXECUTION_ENGINE))
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithNoStoreIdentifier));
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
        .thenReturn(Collections.singletonMap(BATCH_HDFS_OVERRIDE_KEY, Arrays.asList("BQ_BATCH_HDFS")));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("BQ_BATCH_HDFS",storeIdentifier);
  }


  @Test
  public void testFederationReportWithDataSourceAsBIG_QUERYAndStoreIdenfierPresent() {
    Fact fact = createFact(Optional.empty());
    when(factDAO.filterOne(any(),any())).thenReturn(fact);
    QueryPanel queryPanel = QueryPanel.builder().fromTable("test_fact").executionEngine(
        Optional.of(BIG_QUERY_EXECUTION_ENGINE)).build();
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(queryPanel)
        .org("org")
        .executionEngine(Optional.of(BIG_QUERY_EXECUTION_ENGINE))
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Lists.newArrayList(
        reportFederationWithStoreIdentifier));
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
        .thenReturn(Collections.singletonMap(BATCH_HDFS_OVERRIDE_KEY, Arrays.asList("BQ_BATCH_HDFS")));
    Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact",federationForReport,reportSTO.getExecutionEngine());
    Assert.assertEquals("store_identifier",storeIdentifier);
  }

    @Test
    public void testNullFederationForReport() {
        ReportSTO reportSTO = ReportSTO.builder()
                .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
                .org("org")
                .namespace("ns").build();
        when(reportFederationDao.getFederations(any(), any(),any())).thenReturn(Collections.emptyList());
        Optional<ReportFederation> federationForReport = dataService.getFederationForReport(reportSTO, ReportAction.ALL);
        Assert.assertEquals(Optional.empty(), federationForReport);
    }

    /*
    reportFederationWithStoreIdentifier -> bigfoot/test/test_report
    reportFederationWithWildCardNS -> bigfoot/test/*
    reportFederationWithWildCardOrg -> bigfoot/*
    reportFederationWithNoStoreIdentifier -> *
     */
  @Test
  public void testFederationOrder1() {
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(), any())).thenReturn(Lists
        .newArrayList(reportFederationWithWildCardNS,
            reportFederationWithWildCardOrg, reportFederationWithNoStoreIdentifier,
            reportFederationWithStoreIdentifier));
    Optional<ReportFederation> federationForReport = dataService
        .getFederationForReport(reportSTO, ReportAction.ALL);
    Assert.assertEquals(reportFederationWithStoreIdentifier.getReportName(),
        federationForReport.get().getReportName());
  }

  @Test
  public void testFederationOrder2() {
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(), any())).thenReturn(Lists
        .newArrayList(reportFederationWithNoStoreIdentifier, reportFederationWithWildCardNS,
            reportFederationWithWildCardOrg));
    Optional<ReportFederation> federationForReport = dataService
        .getFederationForReport(reportSTO, ReportAction.ALL);
    Assert.assertEquals(reportFederationWithWildCardNS.getReportName(),
        federationForReport.get().getReportName());
  }

  @Test
  public void testFederationOrder3() {
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(), any())).thenReturn(Lists
        .newArrayList(
            reportFederationWithNoStoreIdentifier, reportFederationWithWildCardOrg));
    Optional<ReportFederation> federationForReport = dataService
        .getFederationForReport(reportSTO, ReportAction.ALL);
    Assert.assertEquals(reportFederationWithWildCardOrg.getReportName(),
        federationForReport.get().getReportName());
  }

  @Test
  public void testFederationOrder4() {
    ReportSTO reportSTO = ReportSTO.builder()
        .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build())
        .org("org")
        .namespace("ns").build();
    when(reportFederationDao.getFederations(any(), any(), any())).thenReturn(Lists
        .newArrayList(
            reportFederationWithStoreIdentifier, reportFederationWithWildCardOrg));
    Optional<ReportFederation> federationForReport = dataService
        .getFederationForReport(reportSTO, ReportAction.ALL);
    Assert.assertEquals(reportFederationWithStoreIdentifier.getReportName(),
        federationForReport.get().getReportName());
  }

  @Test
  public void testNullTableFederation(){
    when(tableFederationDao.getFederation(any(), any())).thenReturn(null);
    Map<String,String> datasetInfoMap = new HashMap<>();
    datasetInfoMap.put(SOURCE_PROJECT_ID_KEY,"bq-batch-test-project");
    datasetInfoMap.put(SOURCE_DATASET_NAME_KEY,"batch-dataset");
    when(superbiConfig.getDataSourceAttributes()).thenReturn(Collections.singletonMap("BQ_BATCH_P1",datasetInfoMap));
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
            .thenReturn(Collections.singletonMap(BATCH_SUPPORTED_STORE_IDENTIFIER_KEY, Arrays.asList("BQ_BATCH_P0", "BQ_BATCH_P1")));
    Map<String, String> props = dataService.getBqProperties("b_product.temp_fact", "BQ_BATCH_P1");
    Assert.assertEquals(props.size(), 3);
    Assert.assertEquals(props.get("bq.project_id"),"bq-batch-test-project");
    Assert.assertEquals(props.get("bq.dataset_name"), "batch-dataset");
    Assert.assertEquals(props.get("bq.table_name"), "product__temp_fact");
  }

  @Test
  public void testTableFederationWithAllProps(){
    when(tableFederationDao.getFederation(any(), any())).thenReturn(Lists.newArrayList(tableFederationWithAllProps));
    Map<String, String> props = dataService.getBqProperties(FULLY_QUALIFIED_TABLE_NAME, STORE_IDENTIFIER);
    Assert.assertEquals(props.size(), 2);
    Assert.assertEquals(props.get("bq.dataset_name"), "test_dataset");
    Assert.assertEquals(props.get("bq.table_name"), "test_table");
  }

  @Test
  public void testVerticaSourceStoreIdentifier() {
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
            .thenReturn(Collections.singletonMap(BATCH_OVERRIDE_KEY, Collections.singletonList("BQ_BATCH_P1")));
    Fact fact = createFact(Optional.of(SourceType.VERTICA));
    when(factDAO.filterOne(any(), any())).thenReturn(fact);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact", Optional.empty(),Optional.empty());
    Assert.assertEquals("BQ_BATCH_P1", storeIdentifier);
  }

  @Test
  public void testESSourceStoreIdentifier() {
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
            .thenReturn(Collections.singletonMap(REALTIME_OVERRIDE_KEY, Collections.singletonList("BQ_REALTIME_P1")));
    Fact fact = createFact(Optional.of(SourceType.ELASTIC_SEARCH));
    when(factDAO.filterOne(any(), any())).thenReturn(fact);
    String storeIdentifier = dataService.getStoreIdentifierForFact("test_fact", Optional.empty(),Optional.empty());
    Assert.assertEquals("BQ_REALTIME_P1", storeIdentifier);
  }

  @Test
  public void testBQBatchStoreIdentifierWithoutFederation(){
    when(tableFederationDao.getFederation(any(), any())).thenReturn(null);
    Map<String,String> datasetInfoMap = new HashMap<>();
    datasetInfoMap.put(SOURCE_PROJECT_ID_KEY,"bq-batch-test-project");
    datasetInfoMap.put(SOURCE_DATASET_NAME_KEY,"batch-dataset");
    when(superbiConfig.getDataSourceAttributes()).thenReturn(Collections.singletonMap("BQ_BATCH_P1",datasetInfoMap));
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
            .thenReturn(Collections.singletonMap(BATCH_SUPPORTED_STORE_IDENTIFIER_KEY, Arrays.asList("BQ_BATCH_P0", "BQ_BATCH_P1")));
    Map<String, String> props = dataService.getBqProperties("b_batch_table.BQ_table", "BQ_BATCH_P1");
    Assert.assertEquals(props.size(), 3);
    Assert.assertEquals(props.get("bq.project_id"), "bq-batch-test-project");
    Assert.assertEquals(props.get("bq.dataset_name"), "batch-dataset");
    Assert.assertEquals(props.get("bq.table_name"), "batch_table__bq_table");
  }

  @Test
  public void testBQRealtimeStoreIdentifierWithoutFederation(){
    when(tableFederationDao.getFederation(any(), any())).thenReturn(null);
    Map<String,String> datasetInfoMap = new HashMap<>();
    datasetInfoMap.put(SOURCE_PROJECT_ID_KEY,"bq-realtime-test-project");
    datasetInfoMap.put(SOURCE_DATASET_NAME_KEY,"realtime-dataset");
    when(superbiConfig.getDataSourceAttributes()).thenReturn(Collections.singletonMap("BQ_REALTIME_P0", datasetInfoMap));
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
            .thenReturn(Collections.singletonMap(REALTIME_SUPPORTED_STORE_IDENTIFIER_KEY, Arrays.asList("BQ_REALTIME_P0", "BQ_REALTIME_P1")));
    Map<String, String> props = dataService.getBqProperties("f_realtime_table.BQ_table", "BQ_REALTIME_P0");
    Assert.assertEquals(props.size(), 3);
    Assert.assertEquals(props.get("bq.project_id"), "bq-realtime-test-project");
    Assert.assertEquals(props.get("bq.dataset_name"), "realtime-dataset");
    Assert.assertEquals(props.get("bq.table_name"), "f_realtime_table--BQ_table");
  }

  @Test
  public void testBQBatchHDFSStoreIdentifierWithoutFederation(){
    when(tableFederationDao.getFederation(any(), any())).thenReturn(null);
    Map<String,String> datasetInfoMap = new HashMap<>();
    datasetInfoMap.put(SOURCE_PROJECT_ID_KEY,"fkp-fdp-bq");
    datasetInfoMap.put(SOURCE_DATASET_NAME_KEY,"bqext_bigfoot_external_neo");
    when(superbiConfig.getDataSourceAttributes()).thenReturn(Collections.singletonMap("BQ_BATCH_HDFS",datasetInfoMap));
    when(superbiConfig.getStoreIdentifiersForTableEnrich())
        .thenReturn(Collections.singletonMap(BATCH_HDFS_SUPPORTED_STORE_IDENTIFIER_KEY, Arrays.asList("BQ_BATCH_HDFS")));
    Map<String, String> props = dataService.getBqProperties("b_cp_bi_prod_sales__forward_unit_fact", "BQ_BATCH_HDFS");
    Assert.assertEquals(props.size(), 3);
    Assert.assertEquals(props.get("bq.project_id"), "fkp-fdp-bq");
    Assert.assertEquals(props.get("bq.dataset_name"), "bqext_bigfoot_external_neo");
    Assert.assertEquals(props.get("bq.table_name"), "cp_bi_prod_sales__forward_unit_fact");
  }

  private Fact createFact(Optional<SourceType> sourceType) {
    Source source = new Source();
    source.setName("test_source");
    if (sourceType.isPresent()) {
      source.setSourceType(sourceType.get());
    } else {
      //Source type cannot be null for a fact
      source.setSourceType(SourceType.HDFS);
    }

    Table table = new Table();
    table.setSource(source);

    Fact fact = new Fact();
    fact.setTable(table);
    return fact;
  }

  private void createReportFederations() {
    federationProperties.put("k1","v1");
    federationProperties.put("k2","v2");

    reportFederationWithStoreIdentifier = new ReportFederation("test_fact", "bigfoot/test/test_report",
        ReportAction.ALL, "store_identifier", federationProperties);
    reportFederationWithWildCardNS = new ReportFederation("test_fact", "bigfoot/test/*",
        ReportAction.ALL, "store_identifier", federationProperties);
    reportFederationWithWildCardOrg = new ReportFederation("test_fact", "bigfoot/*",
        ReportAction.ALL, "store_identifier", federationProperties);
    reportFederationWithNoStoreIdentifier = new ReportFederation("test_fact", "*",
        ReportAction.VIEW);

    exactReportNameExactActionName = new ReportFederation("test_fact",
        "bigfoot/test/report1", ReportAction.VIEW);
    exactReportNameExactActionNameWithStoreIdentifier = new ReportFederation("test_fact",
        "bigfoot/test/report1", ReportAction.VIEW, "test_source_P0", Maps.newHashMap());
  }

  private DSQuery getDSQuery() {
    List<SelectColumn> selectColumns = com.google.common.collect.Lists.newArrayList(
        ExprFactory.SEL_COL("event_name").as("event_name").selectColumn,
        new SelectColumn.SimpleColumn("business_unit", "business_unit"),
        ExprFactory.AGGR("gmv", AggregationType.SUM).as("gmv").selectColumn
    );
    String temp_fact_1 = "test_gmv_unit_target_fact";
    return DSQuery.builder()
        .withColumns(selectColumns)
        .withFrom(temp_fact_1)
        .withGroupByColumns(
            com.google.common.collect.Lists.newArrayList("event_name", "business_unit"))
        .withLimit(1)
        .build();
  }

  private void createTableFederation(){
    String tableName = "temp_fact";
    String storeIdentifier = "STORE_IDENTIFIER";
    Map<String, String> props = Maps.newHashMap();
    tableFederationWithNoProps = new TableFederation(tableName, storeIdentifier,props);
    Map<String, String> props_dataset = Maps.newHashMap();
    props_dataset.put("bq.dataset_name", "test_dataset");
    tableFederationWithDatasetName = new TableFederation(tableName, storeIdentifier, props_dataset);
    Map<String, String> props_table = Maps.newHashMap();
    props_table.put("bq.table_name", "test_table");
    tableFederationWithTableName = new TableFederation(tableName, storeIdentifier, props_table);
    Map<String, String> props_all = Maps.newHashMap();
    props_all.put("bq.dataset_name", "test_dataset");
    props_all.put("bq.table_name", "test_table");
    tableFederationWithAllProps = new TableFederation(tableName, storeIdentifier, props_all);

  }
}