package com.flipkart.fdp.superbi.core.service;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.auth.common.exception.NotAuthorizedException;
import com.flipkart.fdp.mmg.cosmos.entities.Source;
import com.flipkart.fdp.mmg.cosmos.entities.SourceType;
import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.core.config.DataPrivilege;
import com.flipkart.fdp.superbi.core.context.ContextProvider;
import com.flipkart.fdp.superbi.core.context.SuperBiContext;
import com.flipkart.fdp.superbi.core.exception.UnMappedUserException;
import com.flipkart.fdp.superbi.core.model.DownloadResponse;
import com.flipkart.fdp.superbi.core.model.ExplainDataResponse;
import com.flipkart.fdp.superbi.core.sto.ReportSTOFactory;
import com.flipkart.fdp.superbi.core.util.DSQueryBuilder;
import com.flipkart.fdp.superbi.dao.ReportDao;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.entities.Report;
import com.flipkart.fdp.superbi.entities.ReportAction;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.qaas.QaasClient;
import com.flipkart.fdp.superbi.http.client.qaas.QaasDownloadResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.flipkart.fdp.superbi.cosmos.utils.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ReportService.class, ContextProvider.class})
public class ReportServiceTest {

    private ReportService reportService;
    @Mock
    private DataService dataService;
    @Mock
    private QaasClient qaasClient;
    @Mock
    private GringottsClient gringottsClient;
    @Mock
    private ReportDao reportDao;
    @Mock
    private ReportSTOFactory factory;
    @Mock
    private Report report;

    @Mock
    private MetricRegistry metricRegistry;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        reportService = new ReportService(reportDao, factory, new HashMap<>(), dataService, gringottsClient, qaasClient, metricRegistry);
    }

    @Test
    public void testDownloadReportWrongSourceType() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        when(dataService.getSourceByFactName(anyString())).thenReturn(createSource(SourceType.ELASTIC_SEARCH));
        exceptionRule.expect(ClientSideException.class);
        spy.downloadReport("test", "test", "test", new HashMap<>());
    }

    @Test
    public void testDownloadReportURLReturned() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        when(dataService.getSourceByFactName(anyString())).thenReturn(createSource(SourceType.HDFS));
        when(dataService.getStoreIdentifierForFact(anyString(),any(),any())).thenReturn("HDFS_DEFAULT");

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();

        when(qaasClient.getDownloadURLForReport(anyString(), anyString(), anyString(), anyString())).thenReturn(new QaasDownloadResponse("someURL"));
        Map<String, String> appliedFilters = new HashMap<>();
        appliedFilters.put("filter1", "filter2");
        when(dataService.getNativeQueryFromQueryPanelResponse(any(), any(), anyString(), any(),any())).thenReturn(ExplainDataResponse.builder().appliedFilters(appliedFilters).build());

        //positive use case
        DownloadResponse response = spy.downloadReport("bigfoot", "test", "myReport", new HashMap<>());
        assertEquals("someURL", response.getUrl());
        assertEquals("QAAS_SERVICE", response.getService());
        assertEquals("filter2", response.getAppliedFilters().get("filter1"));
        assertEquals(1, response.getAppliedFilters().size());
        assertTrue(response.isRedirect());

        //exception scenarios
        doThrow(ServerSideException.class).when(qaasClient).getDownloadURLForReport(anyString(), anyString(), anyString(), anyString());
        exceptionRule.expect(ServerSideException.class);
        spy.downloadReport("bigfoot", "test", "myReport", new HashMap<>());

        doThrow(ClientSideException.class).when(qaasClient).getDownloadURLForReport(anyString(), anyString(), anyString(), anyString());
        exceptionRule.expect(ClientSideException.class);
        spy.downloadReport("bigfoot", "test", "myReport", new HashMap<>());
    }

    @Test
    public void testDownloadReportNoAccess() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();
        when(gringottsClient.hasPrivillege(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(false);

        exceptionRule.expect(NotAuthorizedException.class);
        spy.downloadReport("bigfoot", "test", "myReport", new HashMap<>());
    }

    @Test
    public void testDownloadReportWithoutORG_NS_Mapping() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();
        when(gringottsClient.getBillingLabels(anyString())).thenReturn(null);

        exceptionRule.expect(UnMappedUserException.class);
        exceptionRule.expectMessage("You are not mapped to any org/namespace.");


        spy.downloadReport("bigfoot", "test", "myReport", new HashMap<>());

    }

    @Test
    public void testGetReportDataWithoutORG_NS_Mapping() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();
        when(gringottsClient.getBillingLabels(anyString())).thenReturn(null);

        exceptionRule.expect(UnMappedUserException.class);
        exceptionRule.expectMessage("You are not mapped to any org/namespace");


        spy.getReportData("bigfoot", "test", "myReport", new HashMap<>(), null);
    }

    @Test
    public void testGetReportDataForSystemUserWithoutORG_NS_Mapping() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", Optional.ofNullable("system.user.name"), new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();
        when(gringottsClient.getBillingLabels(anyString())).thenReturn(null);

        exceptionRule.expect(UnMappedUserException.class);
        exceptionRule.expectMessage("You are not mapped to any org/namespace");


        spy.getReportData("bigfoot", "test", "myReport", new HashMap<>(), null);
    }

    @Test
    public void testGetNativeQueryWithoutORG_NS_Mapping() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();
        when(gringottsClient.getBillingLabels(anyString())).thenReturn(null);

        exceptionRule.expect(UnMappedUserException.class);
        exceptionRule.expectMessage("You are not mapped to any org/namespace.");


        spy.getNativeQuery("bigfoot", "test", "myReport", new HashMap<>());
    }

    @Test
    public void testDownloadReportWithBIG_QUERY_EXECUTION_ENGINE() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder()
            .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").executionEngine(
                Optional.of(BIG_QUERY_EXECUTION_ENGINE)).build())
            .org("org")
            .executionEngine(Optional.of(BIG_QUERY_EXECUTION_ENGINE))
            .namespace("ns").build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.BIG_QUERY_BATCH);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);
        when(dataService.getStoreIdentifierForFact(anyString(), any(),any())).thenReturn("BQ_BATCH_HDFS");

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();


        exceptionRule.expect(ClientSideException.class);
        exceptionRule.expectMessage(String.format("Invalid execution flow for source : '%s', report: '%s'.", source.getName(), "myReport"));


        spy.downloadReport("bigfoot", "test", "myReport", new HashMap<>());

    }

    @Test
    public void testGetReportDataWithoutDataSource() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);
        when(dataService.getStoreIdentifierForFact(anyString(), any(),any())).thenReturn("HDFS_DEFAULT");

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();

        spy.getReportData("bigfoot", "test", "myReport", new HashMap<>(), null);
    }

    @Test
    public void testGetReportDataWithBIG_QUERY_ExecutionEngine() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder()
            .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").executionEngine(
                Optional.of(BIG_QUERY_EXECUTION_ENGINE)).build())
            .org("org")
            .executionEngine(Optional.of(BIG_QUERY_EXECUTION_ENGINE))
            .namespace("ns").build();

        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        //set up meta accessor static returns
        Source source = createSource(SourceType.BIG_QUERY_BATCH);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);
        when(dataService.getStoreIdentifierForFact(anyString(), any(),any())).thenReturn("BQ_BATCH_HDFS");

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();

        spy.getReportData("bigfoot", "test", "myReport", new HashMap<>(), null);
    }

    @Test
    public void testGetNativeQueryWithoutExecutionEngineParameter() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder().queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").build()).build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        Source source = createSource(SourceType.HDFS);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);
        when(dataService.getStoreIdentifierForFact(anyString(), any(),any())).thenReturn("HDFS_DEFAULT");

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();

        spy.getNativeQuery("bigfoot", "test", "myReport", new HashMap<>());
    }

    @Test
    public void testGetNativeQueryWithBIG_QUERY_EXECUTION_ENGINE() {
        ReportService spy = Mockito.spy(reportService);
        ReportSTO reportSTO = ReportSTO.builder()
            .queryFormMetaDetailsMap(QueryPanel.builder().fromTable("test_fact").executionEngine(
                Optional.of(BIG_QUERY_EXECUTION_ENGINE)).build())
            .org("org")
            .executionEngine(Optional.of(BIG_QUERY_EXECUTION_ENGINE))
            .namespace("ns").build();
        Mockito.doReturn(reportSTO).when(spy).find(anyString(), anyString(), anyString());

        Source source = createSource(SourceType.BIG_QUERY_BATCH);
        when(dataService.getSourceByFactName(anyString())).thenReturn(source);
        when(dataService.getStoreIdentifierForFact(anyString(), any(), any())).thenReturn("BQ_BATCH_HDFS");

        //user name return
        PowerMockito.mockStatic(ContextProvider.class);
        PowerMockito.when(ContextProvider.getCurrentSuperBiContext()).thenReturn(new SuperBiContext("client1", "user.name", new ClientPrivilege(ReportAction.DOWNLOAD, new DataPrivilege(100, DataPrivilege.LimitPriority.REPORT, true))));

        setUpReportQueryMockReturns();

        spy.getNativeQuery("bigfoot", "test", "myReport", new HashMap<>());
    }

    /*
    If ReportFederation object is empty, federation type will be DEFAULT as per the logic in finding store_identifier
    */
    private void setUpReportQueryMockReturns() {
        when(dataService.getFederationForReport(any(), any())).thenReturn(Optional.empty());
        when(dataService.getStoreIdentifierForFact(anyString(), any(),any())).thenReturn("storeIdentifier");
        when(dataService.getQueryAndParam(any(), any(), any(), any())).thenReturn(new DSQueryBuilder.QueryAndParam(DSQuery.builder().withFrom("test_fact_name").build(),new HashMap<>()));
        when(gringottsClient.hasPrivillege(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(true);
        when(dataService.getNativeQueryFromQueryPanelResponse(any(), any(), anyString(), any(),any())).thenReturn(ExplainDataResponse.builder().build());
    }

    private Source createSource(SourceType sourceType) {
        Source source = new Source();
        source.setName("tableName");
        source.setSourceType(sourceType);
        source.setAttributes("");
        return source;
    }
}