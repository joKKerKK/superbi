package com.flipkart.fdp.superbi.subscription.delivery;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.mail.EmailClient;
import com.flipkart.fdp.superbi.mail.MailResponse;
import com.flipkart.fdp.superbi.subscription.configurations.GsheetConfig;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.exceptions.EmailException;
import com.flipkart.fdp.superbi.subscription.model.EmailDelivery;
import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.EventLog.Event;
import com.flipkart.fdp.superbi.subscription.model.EventLog.State;
import com.flipkart.fdp.superbi.subscription.model.GsheetInfo;
import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.google.inject.internal.cglib.core.$RejectModifierPredicate;
import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import models.ReportEmailRequest;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class EmailExecutor implements DeliveryExecutor {

  private final EmailClient emailClient;
  private final long d42ExpiryInSeconds;
  private final long gsheetExpiryInSeconds;
  private final D42Util d42Util;
  private final GsheetUtil gsheetUtil;
  private final Meter successMeter;
  private final Meter failureMeter;
  private final Meter d42UploadMeter;
  private final Meter gsheetSuccessMeter;
  private final Meter gsheetFailureMeter;
  private final SubscriptionEventLogger subscriptionEventLogger;
  private final ObjectMapper mapper = new ObjectMapper();

  public EmailExecutor(EmailClient emailClient, long d42ExpiryInSeconds,
                       D42Util d42Util, GsheetUtil gsheetUtil, MetricRegistry metricRegistry,
                       SubscriptionEventLogger subscriptionEventLogger, GsheetConfig gsheetConfig) {
    this.emailClient = emailClient;
    this.d42ExpiryInSeconds = d42ExpiryInSeconds;
    this.d42Util = d42Util;
    this.gsheetUtil = gsheetUtil;
    this.successMeter = metricRegistry.meter(getMetricsKeyForSuccess());
    this.failureMeter = metricRegistry.meter(getMetricsKeyForFailure());
    this.d42UploadMeter = metricRegistry.meter(getMetricsKeyForD42Upload());
    this.gsheetSuccessMeter = metricRegistry.meter(getMetricsKeyForGsheetSuccess());
    this.gsheetFailureMeter = metricRegistry.meter(getMetricsKeyForGsheetFailure());
    this.subscriptionEventLogger = subscriptionEventLogger;
    this.gsheetExpiryInSeconds = gsheetConfig.getGsheetExpiryInSeconds();
  }

  @Override
  public void sendContent(ReportDataResponse reportDataResponse,
                          ScheduleInfo scheduleInfo) {
    log.info("sendContent started");
    String link = null;
    ByteArrayOutputStream dataStream = null;
    log.info("scheduleInfo.getDeliveryType() : " + scheduleInfo.getDeliveryType());
    switch (scheduleInfo.getDeliveryType()) {
      case GSHEET:
        log.info("case GSHEET");
        GsheetInfo gsheetInfo = mapper.convertValue(scheduleInfo.getDeliveryConfig(),
            GsheetInfo.class);
        EventLog.Event gsheetEvent =
            (gsheetInfo.getSaveOption().equals(GsheetInfo.SaveOption.NEW_FILE)) ?
                Event.GSHEET_CREATE : Event.GSHEET_OVERWRITE;
        EventLog.EventLogBuilder gsheetEventBuilder =
            subscriptionEventLogger.initiateEventLogBuilder(
                scheduleInfo, gsheetEvent);
        subscriptionEventLogger.startEventAudit(gsheetEventBuilder.build());
        log.info("reportDataResponse.getQueryCachedResult().isTruncated() : " + reportDataResponse.getQueryCachedResult().isTruncated());
        if (reportDataResponse.getQueryCachedResult().isTruncated()) {
          log.info("reportDataResponse.getQueryCachedResult().isTruncated()");
          gsheetEventBuilder.state(State.CANCELLED).message("data exceeds gsheet limit");
          gsheetEventBuilder.completedAt(new Date());
          subscriptionEventLogger.updateEventLogAudit(gsheetEventBuilder.build());

          EventLog.EventLogBuilder d42eventBuilder =
              subscriptionEventLogger.initiateEventLogBuilder(
                  scheduleInfo, Event.D42_UPLOAD);
          subscriptionEventLogger.startEventAudit(d42eventBuilder.build());
          try {
            link = reportDataResponse.getQueryCachedResult().getD42Link();
            if( link != null ) {
              log.info("link 1 : " + link);
            } else {
              log.info("link 1 is NULL ");
            }
            d42eventBuilder.state(State.COMPLETED).content(link);
          } catch (Exception e) {
            log.error(MessageFormat.format("D42 Upload failed for scheduleId <{0}>",
                scheduleInfo.getSubscriptionId()), e);
            d42eventBuilder.message(e.getMessage()).state(State.FAILED);
            throw e;
          } finally {
            d42eventBuilder.completedAt(new Date());
            subscriptionEventLogger.updateEventLogAudit(d42eventBuilder.build());
          }
        } else {
          log.info("!reportDataResponse.getQueryCachedResult().isTruncated()");
          try {

            log.info(MessageFormat.format("Generating Gsheet link for scheduleId <{0}>",
                scheduleInfo.getSubscriptionId()));
            link =
                gsheetUtil.uploadGsheetWithCircuitBreaker(reportDataResponse.getQueryCachedResult().getQueryResult()
                    , scheduleInfo.getReportName(), scheduleInfo, gsheetInfo);
            if( link != null ) {
              log.info("link 2 : " + link);
            } else {
              log.info("link 2 is NULL ");
            }
            gsheetEventBuilder.state(State.COMPLETED).content(link);
            gsheetSuccessMeter.mark();

          } catch (Exception e) {
            log.error(MessageFormat.format("Gsheet Creation failed for scheduleId <{0}>",
                scheduleInfo.getSubscriptionId()), e);
            gsheetEventBuilder.message(e.getMessage()).state(State.FAILED);
            gsheetFailureMeter.mark();
            throw e;
          } finally {
            gsheetEventBuilder.completedAt(new Date());
            subscriptionEventLogger.updateEventLogAudit(gsheetEventBuilder.build());
          }
        }
        break;
      case D42:
        log.info("case D42");
        EventLog.EventLogBuilder d42eventBuilder = subscriptionEventLogger.initiateEventLogBuilder(
            scheduleInfo, Event.D42_UPLOAD);
        subscriptionEventLogger.startEventAudit(d42eventBuilder.build());
        try {
          if (reportDataResponse.getQueryCachedResult().isTruncated()) {
            link = reportDataResponse.getQueryCachedResult().getD42Link();
            if (link != null) {
              log.info("link 1 : " + link);
            } else {
              log.info("link 1 is NULL ");
            }
            d42eventBuilder.state(State.COMPLETED).content(link);
          } else {
            dataStream = generateCsvByteStreamForEmail(reportDataResponse, scheduleInfo);

            log.info(MessageFormat.format("Generating d42 link for scheduleId <{0}>",
                scheduleInfo.getSubscriptionId()));
            link =
                d42Util.uploadDataInD42(reportDataResponse.getQueryCachedResult().getQueryResult()
                    , scheduleInfo.getReportName(), d42ExpiryInSeconds,reportDataResponse.getQueryCachedResult().getCacheKey());
            if (link != null) {
              log.info("link 2 : " + link);
            } else {
              log.info("link 2 is NULL ");
            }
            d42eventBuilder.state(State.COMPLETED).content(link);
            d42UploadMeter.mark();
          }

        } catch (Exception e) {
          log.error(MessageFormat.format("D42 Upload failed for scheduleId <{0}>",
              scheduleInfo.getSubscriptionId()), e);
          d42eventBuilder.message(e.getMessage()).state(State.FAILED);
          throw e;
        } finally {
          d42eventBuilder.completedAt(new Date());
          subscriptionEventLogger.updateEventLogAudit(d42eventBuilder.build());
        }
        break;
      case CSV:
        throw new RuntimeException("CSV Not supported for email");
    }
    EventLog.EventLogBuilder mailEventBuilder = subscriptionEventLogger.initiateEventLogBuilder(
        scheduleInfo, Event.EMAIL);
    subscriptionEventLogger.startEventAudit(mailEventBuilder.build());
    MailResponse mailResponse = sendMail(scheduleInfo, link, dataStream,
        reportDataResponse);
    Boolean isMailSuccess = mailResponse.getIsSuccess();
    if (!isMailSuccess) {
      log.info(MessageFormat.format("Mail was not sent for scheduleId <{0}> due to {1}",
          scheduleInfo.getSubscriptionId(),
          mailResponse.getMessage()));
      failureMeter.mark();
      mailEventBuilder.completedAt(new Date()).message(mailResponse.getMessage()).state(State.FAILED);
      subscriptionEventLogger.updateEventLogAudit(mailEventBuilder.build());
      throw new EmailException(mailResponse.getMessage());
    }
    successMeter.mark();
    mailEventBuilder.completedAt(new Date()).state(State.COMPLETED);
    subscriptionEventLogger.updateEventLogAudit(mailEventBuilder.build());
  }

  private long getExpiryTime(ScheduleInfo scheduleInfo) {
    long expiryTime;
    switch (scheduleInfo.getDeliveryType()) {
      case GSHEET:
        expiryTime = new Date().getTime() + gsheetExpiryInSeconds * 1000;
        break;
      case D42:
        expiryTime = new Date().getTime() + d42ExpiryInSeconds * 1000;
        break;
      case CSV:
        throw new RuntimeException("CSV Not supported for email");
      default:
        throw new IllegalStateException("Unexpected value: " + scheduleInfo.getDeliveryType());
    }
    return expiryTime;
  }

  @Override
  public void sendFailureContent(ScheduleInfo scheduleInfo, Exception e,List<String> subscribers) {
    ReportEmailRequest reportEmailRequest = ReportEmailRequest.builder().toAddressList(subscribers)
        .reportName(scheduleInfo.getReportName()).subscriptionName(scheduleInfo.getSubscriptionName())
        .exception(e.getMessage()).subscriptionId(scheduleInfo.getSubscriptionId()).build();
    emailClient.sendFailureEmail(reportEmailRequest);
  }

  @Override
  public void sendSubscriptionExpiryComm(ScheduleInfo scheduleInfo, List<String> receipients, Date expiryDate) {
    ReportEmailRequest reportEmailRequest = ReportEmailRequest.builder().toAddressList(receipients)
        .reportName(scheduleInfo.getReportName()).subscriptionName(scheduleInfo.getSubscriptionName())
        .expiryDate(expiryDate).subscriptionId(scheduleInfo.getSubscriptionId()).build();
    emailClient.sendCommEmail(reportEmailRequest);
  }

  private String getMetricsKeyForD42Upload() {
    return StringUtils.join(
        Arrays.asList("subscription","d42","count"),'.');
  }

  private String getMetricsKeyForGsheetSuccess() {
    return StringUtils.join(
        Arrays.asList("subscription", "gsheet", "success"), '.');
  }

  private String getMetricsKeyForGsheetFailure() {
    return StringUtils.join(
        Arrays.asList("subscription", "gsheet", "failure"), '.');
  }

  private String getMetricsKeyForSuccess() {
    return StringUtils.join(
        Arrays.asList("subscription","mail","success"),'.');
  }

  private String getMetricsKeyForFailure() {
    return StringUtils.join(Arrays.asList("subscription","mail","failure"),'.');
  }

  private ByteArrayOutputStream generateCsvByteStreamForEmail(ReportDataResponse reportDataResponse, ScheduleInfo scheduleInfo){
    ByteArrayOutputStream byteArrayOutputStream =
        CsvUtil.buildCsvByteStreamFromData(reportDataResponse.getQueryCachedResult().getQueryResult(), scheduleInfo.getReportName());
    return byteArrayOutputStream;
  }


  private MailResponse sendMail(ScheduleInfo scheduleInfo, String d42Link,
                                ByteArrayOutputStream dataStream,
                                ReportDataResponse reportDataResponse) {
    long expiryTime = getExpiryTime(scheduleInfo);
    List<String> subscribers = ((EmailDelivery) scheduleInfo.getDeliveryData()).getSubscribers();
    ReportEmailRequest reportEmailRequest = ReportEmailRequest.builder().toAddressList(subscribers)
        .d42Link(d42Link).expiryDate(new Date(expiryTime)).reportName(scheduleInfo.getReportName())
        .subscriptionId(scheduleInfo.getSubscriptionId()).subscriptionName(scheduleInfo.getSubscriptionName())
        .dataStream(dataStream)
        .numberOfRows(reportDataResponse.getQueryCachedResult().getTotalNumberOfRows())
        .deliveryType(scheduleInfo.getDeliveryType().toString())
        .isTruncated(reportDataResponse.getQueryCachedResult().isTruncated())
        .isOTS(scheduleInfo.getIsOTS()).build();
    if (scheduleInfo.getDeliveryType().equals(ScheduleInfo.DeliveryType.GSHEET)) {
      reportEmailRequest.setDeliveryOption(scheduleInfo.getDeliveryConfig().get("saveOption"));
    }
    return emailClient.sendMail(reportEmailRequest);
  }
}
