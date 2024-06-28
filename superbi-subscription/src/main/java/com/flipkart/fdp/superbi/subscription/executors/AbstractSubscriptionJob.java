package com.flipkart.fdp.superbi.subscription.executors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.subscription.client.PlatoExecutionClient;
import com.flipkart.fdp.superbi.subscription.client.PlatoMetaClient;
import com.flipkart.fdp.superbi.subscription.client.SuperBiClient;
import com.flipkart.fdp.superbi.subscription.delivery.DeliveryExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.exceptions.EmailException;
import com.flipkart.fdp.superbi.subscription.exceptions.TimeoutException;
import com.flipkart.fdp.superbi.subscription.executors.plato.*;
import com.flipkart.fdp.superbi.subscription.model.DeliveryData.DeliveryAction;
import com.flipkart.fdp.superbi.subscription.model.*;
import com.flipkart.fdp.superbi.subscription.model.EventLog.Event;
import com.flipkart.fdp.superbi.subscription.model.EventLog.EventLogBuilder;
import com.flipkart.fdp.superbi.subscription.model.EventLog.State;
import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog;
import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog.ScheduleInfoLogBuilder;
import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog.ScheduleStatus;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;
import com.flipkart.fdp.superbi.subscription.model.plato.PlatoModelDataResponse;
import com.flipkart.fdp.superbi.subscription.model.plato.QueryResult;
import com.flipkart.fdp.superbi.subscription.model.plato.QueryResultMetadata;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractSubscriptionJob implements Job{

  public static final long ONE_DAY = 86400000;
  private final SuperBiClient superBiClient;
  private final PlatoExecutionClient platoExecutionClient;
  private final PlatoMetaClient platoMetaClient;
  private final Map<DeliveryAction,DeliveryExecutor> deliveryExecutorMap;
  private final MetricRegistry metricRegistry;
  private final SubscriptionEventLogger subscriptionEventLogger;
  private final int maxSubscriptionRunsLeftForComm;
  private final int maxDaysLeftForComm;

  final static String FLIPKART_MAIL_ID = "@flipkart.com";

  private static final List<DslModifier> modifiers;
  private static final DslModifierExecutor executor;

  static {
    modifiers = Arrays.asList(
            new LimitModifier(),
            new DefaultTimeColumnRemover(),
            new AdvancedViewFieldsRemover(),
            new MetadataModifier()
    );
    executor = new DslModifierExecutor(modifiers);
  }


  public AbstractSubscriptionJob(SuperBiClient superBiClient
      , PlatoExecutionClient platoExecutionClient, PlatoMetaClient platoMetaClient, Map<DeliveryAction, DeliveryExecutor> deliveryExecutorMap,
                                 MetricRegistry metricRegistry,
                                 SubscriptionEventLogger subscriptionEventLogger, int maxSubscriptionRunsLeftForComm,
                                 int maxDaysLeftForComm ){
    this.superBiClient = superBiClient;
    this.platoExecutionClient = platoExecutionClient;
    this.platoMetaClient = platoMetaClient;
    this.deliveryExecutorMap = deliveryExecutorMap;
    this.metricRegistry = metricRegistry;
    this.subscriptionEventLogger = subscriptionEventLogger;
    this.maxSubscriptionRunsLeftForComm = maxSubscriptionRunsLeftForComm;
    this.maxDaysLeftForComm = maxDaysLeftForComm;
  }

  @Override
  public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    Map<String,Object> jobDataMap = jobExecutionContext.getMergedJobDataMap();
    ScheduleInfo scheduleInfo = JsonUtil.convertValue(jobDataMap,ScheduleInfo.class);
    if(scheduleInfo.getTriggerTime() == null){
      scheduleInfo.setTriggerTime(jobExecutionContext.getScheduledFireTime().getTime());
    }
    if(scheduleInfo.getNextFireTime() == null && jobExecutionContext.getNextFireTime() != null){
      scheduleInfo.setNextFireTime(jobExecutionContext.getNextFireTime().getTime());
    }
    log.info(MessageFormat.format("Triggering subscription for scheduleId <{0}>"
        ,scheduleInfo.getSubscriptionId()));
    EventLog.EventLogBuilder jobEventBuilder = subscriptionEventLogger.initiateEventLogBuilder(scheduleInfo
        ,Event.JOB);
    subscriptionEventLogger.startEventAudit(jobEventBuilder.build());

    Counter activeSchedule = metricRegistry.counter(getCounterKey(scheduleInfo));

    activeSchedule.inc();

    Meter successMeter = metricRegistry.meter(getMetricsKeyForSuccess(scheduleInfo));
    Meter failureMeter = metricRegistry.meter(getMetricsKeyForFailure(scheduleInfo));

    ScheduleInfoLog.ScheduleInfoLogBuilder scheduleInfoLogBuilder = ScheduleInfoLog.builder();
    scheduleInfoLogBuilder.createdAt(new Date()).triggerTime(jobExecutionContext.getScheduledFireTime())
        .scheduleId(scheduleInfo.getSubscriptionId()).scheduleRunId(scheduleInfo.getScheduleRunId())
    .attempt(scheduleInfo.getAttempt()).requestId(scheduleInfo.getRequestId());

    try(Timer.Context timerContext = metricRegistry.timer(getTimerKeyForSubscription(scheduleInfo)).time()){
      executeJob(jobExecutionContext, scheduleInfo,
          scheduleInfoLogBuilder);
      successMeter.mark();
      jobEventBuilder.state(State.COMPLETED);
    }catch (Exception e) {
      log.error(MessageFormat.format("Got exception {0} for scheduleId : {1}",e.getMessage()
          ,scheduleInfo.getSubscriptionId()),e);
      if(checkIfRetryable(scheduleInfo,e)){
        jobEventBuilder.state(State.RETRY).message(e.getMessage());
        scheduleInfoLogBuilder.endAt(new Date()).scheduleStatus(ScheduleStatus.FAILURE).message(e.getMessage());
        submitForRetry(e,scheduleInfo,jobExecutionContext);
      }
      else{
        jobEventBuilder.state(State.FAILED).message(e.getMessage());
        scheduleInfoLogBuilder.endAt(new Date()).scheduleStatus(ScheduleStatus.FAILURE ).message(e.getMessage());
        failureMeter.mark();
        if(scheduleInfo.getDeliveryData().getDeliveryAction().equals(DeliveryAction.EMAIL) && !(e instanceof EmailException)){
          deliveryExecutorMap.get(scheduleInfo.getDeliveryData().getDeliveryAction()).sendFailureContent(scheduleInfo,e
          ,((EmailDelivery)scheduleInfo.getDeliveryData()).getSubscribers());
        }else{
          deliveryExecutorMap.get(DeliveryAction.EMAIL).sendFailureContent(scheduleInfo,e
              , Collections.singletonList(scheduleInfo.getOwnerId().concat(FLIPKART_MAIL_ID)));
        }
      }
    }
    finally{
      try {
        if (!scheduleInfo.getIsOTS() && isSubscriptionOnLastRuns(jobExecutionContext, scheduleInfo)) {
          List<String> recipients;
          if(scheduleInfo.getDeliveryData().getDeliveryAction().equals(DeliveryAction.EMAIL)) {
            recipients = ((EmailDelivery)scheduleInfo.getDeliveryData()).getSubscribers();
          }
          else {
            recipients = ((FTPDelivery)scheduleInfo.getDeliveryData()).getCommEmails();
          }
          if(recipients == null || recipients.isEmpty()) {
            recipients = Collections.singletonList(scheduleInfo.getOwnerId().concat(FLIPKART_MAIL_ID));
          }

          deliveryExecutorMap.get(DeliveryAction.EMAIL).sendSubscriptionExpiryComm(scheduleInfo, recipients,
              jobExecutionContext.getTrigger().getEndTime());
        }
      } catch (Exception e) {
        log.error(MessageFormat.format("Unable to send expiry comm for scheduleId : <{0}> due to exception <{1}>",scheduleInfo.getSubscriptionId(), e.getMessage()));
      }
      subscriptionEventLogger.updateEventLogAudit(jobEventBuilder.completedAt(new Date()).build());
      activeSchedule.dec();
    }
  }

  private boolean isSubscriptionOnLastRuns(JobExecutionContext jobExecutionContext, ScheduleInfo scheduleInfo) throws ParseException {

    CronExpression cronExpression = new CronExpression(scheduleInfo.getCron());
    Date executionDate = jobExecutionContext.getFireTime();
    Date nextFireDate = executionDate;

    for(int i = 0; i < maxSubscriptionRunsLeftForComm; i++) {
      nextFireDate = cronExpression.getNextValidTimeAfter(nextFireDate);
    }
    if(executionDate.getTime() + maxDaysLeftForComm * ONE_DAY > jobExecutionContext.getTrigger().getEndTime().getTime() &&
    nextFireDate.getTime() > jobExecutionContext.getTrigger().getEndTime().getTime()) {
      return true;
    }
    return false;
  }

  @SneakyThrows
  private void executeJob(JobExecutionContext jobExecutionContext, ScheduleInfo scheduleInfo, ScheduleInfoLogBuilder scheduleInfoLogBuilder){
    long scheduledTime = scheduleInfo.getTriggerTime() == null ? jobExecutionContext.getScheduledFireTime().getTime()
        : scheduleInfo.getTriggerTime();

    EventLog.EventLogBuilder apiEventBuilder = subscriptionEventLogger.initiateEventLogBuilder(scheduleInfo,Event.DATA_CALL);
    subscriptionEventLogger.startEventAudit(apiEventBuilder.build());

    ReportDataResponse reportDataResponse;
    try{
      switch (scheduleInfo.getResourceType()) {
        case REPORT:
          reportDataResponse = superBiClient.getDataForSubscription(scheduleInfo);
          handleApiResponse(scheduleInfo, scheduleInfoLogBuilder, scheduledTime,
                  apiEventBuilder, reportDataResponse);
          deliveryExecutorMap.get(scheduleInfo.getDeliveryData().getDeliveryAction()).sendContent(reportDataResponse,scheduleInfo);
          break;
        case CANVAS:
          // TODO: get widgets for all tabs
          log.info(MessageFormat.format("Executing subscription schedule for widget <{0}>" ,scheduleInfo.getWidgets()));
          Optional<Canvas.Tab.Widget> widget = platoMetaClient.getWidget(Long.valueOf(scheduleInfo.getWidgets()), scheduleInfo.getOwnerId());

          if (!widget.isPresent()) throw new IllegalStateException(MessageFormat.format("Widget With id {0} does not exist", scheduleInfo.getWidgets()));

          PlatoModelDataResponse platoModelDataResponse = platoExecutionClient.getModelData(scheduleInfo, buildPlatoExecutionModelRequest(widget.get()));
          handleApiResponse(scheduleInfo, scheduleInfoLogBuilder, scheduledTime,
                  apiEventBuilder, platoModelDataResponse);
          deliveryExecutorMap.get(scheduleInfo.getDeliveryData().getDeliveryAction()).sendContent(transformToReportResponse(platoModelDataResponse),scheduleInfo);
          break;
        default:
          throw new IllegalArgumentException("Unknown resource type");
      }
      scheduleInfoLogBuilder.endAt(new Date()).scheduleStatus(ScheduleStatus.SUCCESS);

    }catch (Exception e){
      apiEventBuilder.completedAt(new Date()).message(e.getMessage()).state(State.FAILED).content(scheduleInfo.getRequestId());
      subscriptionEventLogger.updateEventLogAudit(apiEventBuilder.build());
      throw e;
    }
  }


  private String buildPlatoExecutionModelRequest(Canvas.Tab.Widget widget) throws IOException {
    try {
      String model = widget.getModel();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(model);

      return executor.executeModifiers(rootNode, mapper, widget);
    } catch (IOException e) {
      log.error("Failed to build Plato execution model request with DSL modifiers", e);
      return widget.getModel();
    }

  }
  private ReportDataResponse transformToReportResponse(PlatoModelDataResponse platoModelData) {

    QueryResultMetadata queryResultMetadata = platoModelData.getResponseMetadata().get();

    QueryResult queryResult = platoModelData.getQueryResult().get();

    // Create a ReportDataResponse object
    ReportDataResponse reportDataResponse = ReportDataResponse.builder()
            .queryCachedResult(QueryResultCachedValue.builder()
                    .cacheKey(queryResultMetadata.getCacheKey())
                    .cachedAtTime(queryResultMetadata.getFreshAsOf())
                    .queryResult(new RawQueryResultWithSchema(
                            queryResult.getResults(),
                            null,  // dateHistogramMeta is not available in PlatoModelDataResponse
                            combineSchemaList(queryResult.getSchema())
                    ))
                    .totalNumberOfRows(queryResultMetadata.getTotalNumberOfRows().or(0))
                    .truncatedRows(queryResultMetadata.getTruncatedRows().or(0))
                    .truncated(queryResultMetadata.getTruncated())
                    .d42Link(queryResultMetadata.getD42Link().orNull())
                    .build())
            .freshAsOf(queryResultMetadata.getFreshAsOf())
            .build();

    logReportDataResponse(reportDataResponse);
    return reportDataResponse;
  }

  private void logReportDataResponse(ReportDataResponse reportDataResponse) {
    try {
      log.info("ReportDataResponse:");

      // Log fields of queryCachedResult
      if (reportDataResponse.getQueryCachedResult() != null) {
        QueryResultCachedValue queryCachedResult = reportDataResponse.getQueryCachedResult();
        log.info(" - queryCachedResult:");
        log.info("   - totalNumberOfRows: " + queryCachedResult.getTotalNumberOfRows());
        log.info("   - truncatedRows: " + queryCachedResult.getTruncatedRows());
        log.info("   - truncated: " + queryCachedResult.isTruncated());
        log.info("   - d42Link: " + queryCachedResult.getD42Link());

        // Log fields of queryResult
        if (queryCachedResult.getQueryResult() != null) {
          RawQueryResultWithSchema queryResult = queryCachedResult.getQueryResult();
          log.info("   - queryResult:");
          log.info("     - data: " + queryResult.getData());
        }
      }
    } catch (Exception e) {
      log.error("An error occurred while logging ReportDataResponse: " + e);
    }
  }

  private Schema combineSchemaList(List<QueryResult.SchemaInfo> schema) {
    List<SelectColumn> selectColumns = schema.stream().map(schemaInfo -> new SelectColumn.SimpleColumn(schemaInfo.getColumnAlias(), schemaInfo.getColumnAlias())).collect(Collectors.toList());
      return new Schema(DSQuery.builder().withFrom("").withColumns(selectColumns).build(), new HashMap<>(), false);
  }

  private void handleApiResponse(ScheduleInfo scheduleInfo, ScheduleInfoLogBuilder scheduleInfoLogBuilder, long scheduledTime, EventLogBuilder apiEventBuilder, PlatoModelDataResponse platoModelDataResponse) {

  }

  private void handleApiResponse(ScheduleInfo scheduleInfo,
      ScheduleInfoLogBuilder scheduleInfoLogBuilder, long scheduledTime,
      EventLogBuilder apiEventBuilder, ReportDataResponse reportDataResponse) {

    log.info(MessageFormat.format("Got freshAsOf {0} and scheduleTime {1} for scheduleId <{2}>"
        ,reportDataResponse.getFreshAsOf(),scheduledTime,scheduleInfo.getSubscriptionId()));

    if(!isPollRequired(reportDataResponse.getFreshAsOf(),scheduledTime)){
      log.info(MessageFormat.format("Got successful response for scheduleId -> <{0}>",scheduleInfo.getSubscriptionId()));

      apiEventBuilder.completedAt(new Date()).state(State.COMPLETED).content(scheduleInfo.getRequestId());
      subscriptionEventLogger.updateEventLogAudit(apiEventBuilder.build());

    }else{
      if(checkIfReportIsLocked(reportDataResponse,scheduledTime)){
        log.warn(MessageFormat.format("Report {0} is locked due to {1} for scheduleId <{2}>"
            ,scheduleInfo.getReportName(),reportDataResponse.getAttemptInfo().getErrorMessage(),scheduleInfo.getSubscriptionId()));
        throw new ClientSideException(MessageFormat.format("Report {0} is locked due to {1}"
            ,scheduleInfo.getReportName(),reportDataResponse.getAttemptInfo().getErrorMessage()));
      }
      log.warn(MessageFormat.format("Api doesn't return response for scheduleId <{0}>",scheduleInfo.getSubscriptionId()));
      throw new TimeoutException("A timeout has occurred while processing this request");
    }
  }

  private boolean checkIfReportIsLocked(ReportDataResponse reportDataResponse,long scheduledTime) {
    return reportDataResponse.getAttemptInfo() != null
        && reportDataResponse.getAttemptInfo().getErrorMessage() != null
        && reportDataResponse.getAttemptInfo().getCachedAtTime() > scheduledTime
        && !reportDataResponse.getAttemptInfo().isServerError();
  }

  public Boolean checkIfRetryable(ScheduleInfo scheduleInfo, Exception e) {
    final long currentTime = new Date().getTime();
    scheduleInfo.setAttempt(scheduleInfo.getAttempt() + 1);
    log.info(MessageFormat.format("Checking to retry for scheduleId <{0}> and  exception <{1}>"
        ,scheduleInfo.getSubscriptionId(),e.getMessage()));
    return !((scheduleInfo.getNextFireTime() != null &&
    currentTime > scheduleInfo.getNextFireTime()) || currentTime > scheduleInfo.getTriggerTime() + ONE_DAY
        || e instanceof ClientSideException);
  }

  protected abstract void submitForRetry(Exception e,ScheduleInfo scheduleInfo
      ,JobExecutionContext jobExecutionContext) throws JobExecutionException;

  public static boolean isPollRequired(long freshAsOf,long scheduledTime){
    return scheduledTime > freshAsOf;
  }

  private String getMetricsKeyForSuccess(ScheduleInfo scheduleInfo) {
    return StringUtils.join(
        Arrays.asList("subscription",scheduleInfo.getDeliveryData().getDeliveryAction(),"success"),'.');
  }

  private String getMetricsKeyForFailure(ScheduleInfo scheduleInfo) {
    return StringUtils.join(Arrays.asList("subscription",scheduleInfo.getDeliveryData().getDeliveryAction(),"failure"),'.');
  }

  private String getTimerKeyForSubscription(ScheduleInfo scheduleInfo) {
    return StringUtils.join(Arrays.asList("subscription",
        scheduleInfo.getDeliveryData().getDeliveryAction(),"timer"),'.');
  }

  private String getCounterKey(ScheduleInfo scheduleInfo) {
    return StringUtils.join(Arrays.asList("subscription","active",scheduleInfo.getDeliveryData().getDeliveryAction(),"counter"),'.');
  }

}
