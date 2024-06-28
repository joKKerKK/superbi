package com.flipkart.fdp.superbi.core.adaptor;

import static com.flipkart.fdp.superbi.core.adaptor.BackgroundQueryExecutorAdaptor.DEFAULT_PRIORITY;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.es.client.ESQuery.QueryType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClient;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClient;
import com.flipkart.fdp.superbi.refresher.api.execution.BackgroundRefresher;
import com.google.common.collect.Maps;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by akshaya.sharma on 29/05/20
 */
@Slf4j
public class BackgroundQueryExecutorAdaptorTest {
  private static final String TERM_VIEW_PRIORITY = "TERM_VIEW";
  private static final String AGGR_VIEW_PRIORITY = "AGGR_VIEW";
  private static final String TERM_DOWNLOAD_PRIORITY = "TERM_DOWNLOAD";
  private static final String AGGR_DOWNLOAD_PRIORITY = "AGGR_DOWNLOAD";

  MmgClient mmgClient = Mockito.mock(MmgClient.class);

  BadgerClient badgerClient = Mockito.mock(BadgerClient.class);
  
  BackgroundRefresher backgroundRefresher = Mockito.mock(BackgroundRefresher.class);
  Map<String, AbstractDSLConfig> dslConfigMap = Maps.newHashMap();
  MetricRegistry metricRegistry = Mockito.mock(MetricRegistry.class);
  Function<String, Boolean> checkDSQuerySerialization = (storeIdentifier) -> false;

  Function<String, Boolean> shouldCalculatePriority = (storeIdentifier) -> "allowedStoreIdentifier".equals(storeIdentifier);

  Function<ESQuery.QueryType, Long> getElasticSearchCostBoost = (queryType) -> queryType.equals(QueryType.TERM) ? 2L: 1L;

  Function<String, Boolean> factRefreshTimeRequiredProvider = (storeIdentifier) -> !"refreshNotRequiredStore".equals(storeIdentifier);

  BackgroundQueryExecutorAdaptor backgroundQueryExecutorAdaptor = new BackgroundQueryExecutorAdaptor(
      backgroundRefresher, dslConfigMap, metricRegistry, checkDSQuerySerialization, shouldCalculatePriority
      ,mmgClient,
      getElasticSearchCostBoost,factRefreshTimeRequiredProvider, badgerClient);

  @Test
  public void calculatePriorityOnlyIfEnabled() {
    ESQuery termQuery = getSampleTermQuery();
    AdaptorQueryPayload adaptorQueryPayload = Mockito.mock(AdaptorQueryPayload.class);
    Mockito.when(adaptorQueryPayload.getStoreIdentifier()).thenReturn("notAllowedStoreIdentifier");
    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("VIEW");
    String priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, termQuery);

    Assert.assertEquals(DEFAULT_PRIORITY, priority);
  }

  @Test
  public void calculatePriorityForTermQuery() {
    ESQuery termQuery = getSampleTermQuery();
    AdaptorQueryPayload adaptorQueryPayload = Mockito.mock(AdaptorQueryPayload.class);
    Mockito.when(adaptorQueryPayload.getStoreIdentifier()).thenReturn("allowedStoreIdentifier");
    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("VIEW");
    String priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, termQuery);

    Assert.assertEquals(TERM_VIEW_PRIORITY, priority);

    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("DOWNLOAD");
    priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, termQuery);
    Assert.assertEquals(TERM_DOWNLOAD_PRIORITY, priority);
  }

  @Test
  public void calculatePriorityForAggQuery() {
    ESQuery aggQuery = getSampleAggQuery();
    AdaptorQueryPayload adaptorQueryPayload = Mockito.mock(AdaptorQueryPayload.class);
    Mockito.when(adaptorQueryPayload.getStoreIdentifier()).thenReturn("allowedStoreIdentifier");
    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("VIEW");
    String priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, aggQuery);

    Assert.assertEquals(AGGR_VIEW_PRIORITY, priority);

    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("DOWNLOAD");
    priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, aggQuery);
    Assert.assertEquals(AGGR_DOWNLOAD_PRIORITY, priority);
  }

  @Test
  public void defaultPriorityForNonESQuery() {
    Object nativeSql = "select * from dummy";
    AdaptorQueryPayload adaptorQueryPayload = Mockito.mock(AdaptorQueryPayload.class);
    Mockito.when(adaptorQueryPayload.getStoreIdentifier()).thenReturn("allowedStoreIdentifier");
    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("VIEW");
    String priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, nativeSql);

    Assert.assertEquals(DEFAULT_PRIORITY, priority);

    Mockito.when(adaptorQueryPayload.getReportAction()).thenReturn("DOWNLOAD");
    priority = backgroundQueryExecutorAdaptor.getPriority(adaptorQueryPayload, nativeSql);
    Assert.assertEquals(DEFAULT_PRIORITY, priority);
  }

  @Test
  public void testCostForESQuery() {
    ESQuery aggQuery = getSampleAggQuery();
    long costBoost = backgroundQueryExecutorAdaptor.getCostBoost(aggQuery);
    Assert.assertEquals(1, costBoost);

    ESQuery termQuery = getSampleTermQuery();
    costBoost = backgroundQueryExecutorAdaptor.getCostBoost(termQuery);
    Assert.assertEquals(2, costBoost);
  }

  private ESQuery getSampleTermQuery() {
    return ESQuery.builder()
        .queryType(ESQuery.QueryType.TERM)
        .build();
  }

  private ESQuery getSampleAggQuery() {
    return ESQuery.builder()
        .queryType(ESQuery.QueryType.AGGR)
        .build();
  }

  @Test
  public void testFactRefreshTimeResponseCase(){
    Mockito.when(mmgClient.getFactRefreshTime("fact", "storeIdentifier")).thenReturn(100L);
    Long factRefreshTime = backgroundQueryExecutorAdaptor.getFactRefreshTime("fact","store");
    Assert.assertEquals(factRefreshTime,Long.valueOf("100"));
  }

  @Test
  public void testFactRefreshTimeWithException(){
    Mockito.when(mmgClient.getFactRefreshTime("fact", "storeIdentifier")).thenThrow(new ServerSideException("message"));
    Long factRefreshTime = backgroundQueryExecutorAdaptor.getFactRefreshTime("fact","store");
    Assert.assertTrue(factRefreshTime > new Date().getTime() - 10000);
  }

  @Test
  public void testFactRefreshTimeWithMmgNotCalled(){
    Mockito.when(mmgClient.getFactRefreshTime("fact", "storeIdentifier")).thenThrow(new ServerSideException("message"));
    Long factRefreshTime = backgroundQueryExecutorAdaptor.getFactRefreshTime("fact","refreshNotRequiredStore");
    Assert.assertEquals(factRefreshTime ,Long.valueOf(0));
  }
}
