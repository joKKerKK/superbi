package com.flipkart.fdp.superbi.subscription.executors;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.subscription.client.PlatoExecutionClient;
import com.flipkart.fdp.superbi.subscription.client.PlatoMetaClient;
import com.flipkart.fdp.superbi.subscription.client.SuperBiClient;
import com.flipkart.fdp.superbi.subscription.delivery.DeliveryExecutor;
import com.flipkart.fdp.superbi.subscription.delivery.EmailExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.model.AttemptInfo;
import com.flipkart.fdp.superbi.subscription.model.DeliveryData.DeliveryAction;
import com.flipkart.fdp.superbi.subscription.model.EmailDelivery;
import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.QueryResultCachedValue;
import com.flipkart.fdp.superbi.subscription.model.RawQueryResultWithSchema;
import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo.DeliveryType;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo.ResourceType;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

@RunWith(PowerMockRunner.class)
public class SubscriptionJobTest {

  @Mock
  private SuperBiClient superBiClient;

  @Mock
  private PlatoExecutionClient platoExecutionClient;

  @Mock
  private PlatoMetaClient platoMetaClient;

  @Mock
  private JobExecutionContext jobExecutionContext;

  @Mock
  private Trigger trigger;

  private JobDataMap jobDataMap;

  private Map<DeliveryAction,DeliveryExecutor> deliveryExecutorMap;

  @Mock
  private EmailExecutor emailExecutor;

  private SubscriptionJob subscriptionJob;

  private MetricRegistry metricRegistry = new MetricRegistry();

  private EventLog.EventLogBuilder eventLogBuilder = EventLog.builder();

  @Mock
  private RetryJobHandler retryJobHandler;

  @Mock
  private SubscriptionEventLogger subscriptionEventLogger;

  @Before
  public void setUp(){
    deliveryExecutorMap = new HashMap<>();
    deliveryExecutorMap.put(DeliveryAction.EMAIL,emailExecutor);
    Mockito.doNothing().when(emailExecutor).sendContent(Mockito.any(),Mockito.any());
    jobDataMap = getJobDataMapForEmailAction();
    subscriptionJob = new SubscriptionJob(superBiClient,deliveryExecutorMap,
        retryJobHandler,metricRegistry,subscriptionEventLogger, 7, 60, platoExecutionClient, platoMetaClient);
  }

  @Test
  public void testIsPollRequiredLatestFreshOf(){
    long freshAsOf = new Date().getTime();
    long scheduledTime = new Date().getTime() - 1000;
    boolean isPollRequired = SubscriptionJob.isPollRequired(freshAsOf,scheduledTime);
    Assert.assertEquals(isPollRequired,false);
  }

  @Test
  public void testIsPollRequiredStaleData(){
    long freshAsOf = new Date().getTime() - 1000;
    long scheduledTime = new Date().getTime();
    boolean isPollRequired = SubscriptionJob.isPollRequired(freshAsOf,scheduledTime);
    Assert.assertEquals(isPollRequired,true);
  }

  @Test
  public void testSuperBiClientWithLatestData() throws Exception {
    long currentTime = new Date().getTime();
    ReportDataResponse reportDataResponse = getReportDataResponse(currentTime,currentTime);
    Mockito.when(superBiClient.getDataForSubscription(Mockito.any())).thenReturn(reportDataResponse);
    Mockito.when(jobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
    Mockito.when(jobExecutionContext.getScheduledFireTime()).thenReturn(new Date(currentTime));
    Mockito.when(jobExecutionContext.getNextFireTime()).thenReturn(new Date(currentTime));
    Mockito.doNothing().when(retryJobHandler).submitForRetry(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).startEventAudit(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).updateEventLogAudit(Mockito.any());
    Mockito.when(subscriptionEventLogger.initiateEventLogBuilder(Mockito.any(),Mockito.any())).thenReturn(eventLogBuilder);
    subscriptionJob.execute(jobExecutionContext);

    Mockito.verify(superBiClient,Mockito.times(1));
  }

  @Test
  public void testSuperBiClientWithStaleAndThenLatestData() throws Exception{

    long currentTime = new Date().getTime();
    ReportDataResponse reportDataResponseStale = getReportDataResponse(currentTime - 1000,currentTime -1000);
    ReportDataResponse reportDataResponseLatest = getReportDataResponse(currentTime,currentTime);
    Mockito.when(superBiClient.getDataForSubscription(Mockito.any()))
        .thenReturn(reportDataResponseStale).thenReturn(reportDataResponseLatest);
    Mockito.when(jobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
    Mockito.when(jobExecutionContext.getScheduledFireTime()).thenReturn(new Date(currentTime));
    Mockito.when(jobExecutionContext.getNextFireTime()).thenReturn(new Date(currentTime));
    Mockito.doNothing().when(retryJobHandler).submitForRetry(Mockito.any());
    Mockito.when(subscriptionEventLogger.initiateEventLogBuilder(Mockito.any(),Mockito.any())).thenReturn(eventLogBuilder);
    Mockito.doNothing().when(subscriptionEventLogger).startEventAudit(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).updateEventLogAudit(Mockito.any());
    subscriptionJob.execute(jobExecutionContext);

    Mockito.verify(superBiClient,Mockito.times(2));

  }

  @Test
  public void testSuperBiClientWithPollAndThenLatestData() throws Exception{

    long currentTime = new Date().getTime();
    ReportDataResponse reportDataResponsePoll = getReportDataResponse(0,0);
    ReportDataResponse reportDataResponseLatest = getReportDataResponse(currentTime,currentTime);
    Mockito.when(superBiClient.getDataForSubscription(Mockito.any()))
        .thenReturn(reportDataResponsePoll).thenReturn(reportDataResponsePoll).thenReturn(reportDataResponseLatest);
    Mockito.when(jobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
    Mockito.when(jobExecutionContext.getScheduledFireTime()).thenReturn(new Date(currentTime));
    Mockito.when(jobExecutionContext.getNextFireTime()).thenReturn(new Date(currentTime));
    Mockito.doNothing().when(retryJobHandler).submitForRetry(Mockito.any());
    Mockito.when(subscriptionEventLogger.initiateEventLogBuilder(Mockito.any(),Mockito.any())).thenReturn(eventLogBuilder);
    Mockito.doNothing().when(subscriptionEventLogger).startEventAudit(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).updateEventLogAudit(Mockito.any());
    subscriptionJob.execute(jobExecutionContext);

    Mockito.verify(superBiClient,Mockito.times(3));

  }

  @Test
  public void testSuperBiClientWithStaleValidData() throws Exception{

    long currentTime = new Date().getTime();
    ReportDataResponse reportDataResponseStale = getReportDataResponse(currentTime - 1000,currentTime -1000);
    ReportDataResponse reportDataResponseLatest = getReportDataResponse(currentTime,currentTime);
    Mockito.when(superBiClient.getDataForSubscription(Mockito.any()))
        .thenReturn(reportDataResponseStale).thenReturn(reportDataResponseLatest);
    Mockito.when(jobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
    Mockito.when(jobExecutionContext.getScheduledFireTime()).thenReturn(new Date(currentTime - 10000));
    Mockito.when(jobExecutionContext.getNextFireTime()).thenReturn(new Date(currentTime));
    Mockito.doNothing().when(retryJobHandler).submitForRetry(Mockito.any());
    Mockito.when(subscriptionEventLogger.initiateEventLogBuilder(Mockito.any(),Mockito.any())).thenReturn(eventLogBuilder);
    Mockito.doNothing().when(subscriptionEventLogger).startEventAudit(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).updateEventLogAudit(Mockito.any());

    subscriptionJob.execute(jobExecutionContext);

    Mockito.verify(superBiClient,Mockito.times(1));

  }

  @Test
  public void testSuperBiClientWithException() throws Exception{

    long currentTime = new Date().getTime();
    ReportDataResponse reportDataResponseStale = getReportDataResponse(currentTime - 1000,currentTime -1000);
    Mockito.when(superBiClient.getDataForSubscription(Mockito.any()))
        .thenReturn(reportDataResponseStale).thenThrow(new RuntimeException());
    Mockito.when(jobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
    Mockito.when(jobExecutionContext.getScheduledFireTime()).thenReturn(new Date(currentTime - 10000));
    Mockito.when(jobExecutionContext.getNextFireTime()).thenReturn(new Date(currentTime));
    Mockito.when(trigger.getKey()).thenReturn(new TriggerKey(""));
    Mockito.when(jobExecutionContext.getTrigger()).thenReturn(trigger);
    Mockito.when(subscriptionEventLogger.initiateEventLogBuilder(Mockito.any(),Mockito.any())).thenReturn(eventLogBuilder);
    Mockito.doNothing().when(retryJobHandler).submitForRetry(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).startEventAudit(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).updateEventLogAudit(Mockito.any());
    subscriptionJob.execute(jobExecutionContext);

    Mockito.verify(superBiClient,Mockito.times(2));

  }

  @Test
  public void testSuperbiClientReportLockedWithNoData() throws JobExecutionException {
    long currentTime = new Date().getTime();
    AttemptInfo attemptInfo = AttemptInfo.builder().errorMessage(null).build();
    ReportDataResponse reportDataResponsePoll = getReportDataResponseWithAttemptInfo(0,currentTime,attemptInfo);
    Mockito.when(superBiClient.getDataForSubscription(Mockito.any()))
        .thenReturn(reportDataResponsePoll).thenReturn(reportDataResponsePoll);
    Mockito.when(jobExecutionContext.getMergedJobDataMap()).thenReturn(jobDataMap);
    Mockito.when(jobExecutionContext.getScheduledFireTime()).thenReturn(new Date(currentTime));
    Mockito.when(jobExecutionContext.getNextFireTime()).thenReturn(new Date(currentTime));
    Mockito.doNothing().when(retryJobHandler).submitForRetry(Mockito.any());
    Mockito.when(subscriptionEventLogger.initiateEventLogBuilder(Mockito.any(),Mockito.any())).thenReturn(eventLogBuilder);
    Mockito.doNothing().when(subscriptionEventLogger).startEventAudit(Mockito.any());
    Mockito.doNothing().when(subscriptionEventLogger).updateEventLogAudit(Mockito.any());
    subscriptionJob.execute(jobExecutionContext);
    Mockito.verify(superBiClient,Mockito.times(1));
  }

  private JobDataMap getJobDataMapForEmailAction(){

    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put("org","");
    jobDataMap.put("namespace","");
    jobDataMap.put("reportName","");
    jobDataMap.put("ownerId","");
    jobDataMap.put("deliveryData", new EmailDelivery(new ArrayList<>(),DeliveryAction.EMAIL));
    jobDataMap.put("subscriptionId", 1);
    jobDataMap.put("subscriptionName", "subscriptionName");
    jobDataMap.put("subscriptionId", 1L);
    jobDataMap.put("resourceType", ResourceType.REPORT.toString());
    jobDataMap.put("deliveryType", DeliveryType.D42.toString());

    return jobDataMap;
  }

  private ReportDataResponse getReportDataResponse(long cachedAtTime,long freshAsOf) {
    Schema schema = new Schema(ImmutableList.copyOf(new ArrayList<SelectColumn>()),null,null,10);
    RawQueryResultWithSchema rawQueryResult = new RawQueryResultWithSchema(new ArrayList<>(),new HashMap<>(),schema);
    QueryResultCachedValue queryResultCached = new QueryResultCachedValue("",cachedAtTime,"",rawQueryResult,
        300,300,false,"","");
    return new ReportDataResponse(queryResultCached,null,freshAsOf);
  }


  private ReportDataResponse getReportDataResponseWithAttemptInfo(long cachedAtTime,long freshAsOf,AttemptInfo attemptInfo) {
    Schema schema = new Schema(ImmutableList.copyOf(new ArrayList<SelectColumn>()),null,null,10);
    RawQueryResultWithSchema rawQueryResult = new RawQueryResultWithSchema(new ArrayList<>(),new HashMap<>(),schema);
    QueryResultCachedValue queryResultCached = new QueryResultCachedValue("",cachedAtTime,"",rawQueryResult,
        300,300,false,"","");
    return new ReportDataResponse(queryResultCached,attemptInfo,freshAsOf);
  }

}
