package com.flipkart.fdp.superbi.subscription.delivery;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.model.CSVStream;
import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.EventLog.Event;
import com.flipkart.fdp.superbi.subscription.model.EventLog.EventLogBuilder;
import com.flipkart.fdp.superbi.subscription.model.FTPDelivery;
import com.flipkart.fdp.superbi.subscription.model.RawQueryResultWithSchema;
import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

@Slf4j
public class FtpExecutor implements DeliveryExecutor {

  private final D42Client d42Client;
  private final Meter successMeter;
  private final Meter failureMeter;
  private final SubscriptionEventLogger subscriptionEventLogger;
  private static final String tmp_extension = ".__TMP__";
  private final Event event;

  public FtpExecutor(D42Client d42Client, MetricRegistry metricRegistry,
      SubscriptionEventLogger subscriptionEventLogger, Event event) {
    this.d42Client = d42Client;
    this.successMeter = metricRegistry.meter(getMetricsKeyForSuccess());
    this.failureMeter = metricRegistry.meter(getMetricsKeyForFailure());
    this.subscriptionEventLogger = subscriptionEventLogger;
    this.event = event;
  }

  public FtpExecutor(D42Client d42Client, MetricRegistry metricRegistry,
      SubscriptionEventLogger subscriptionEventLogger) {
    this(d42Client, metricRegistry, subscriptionEventLogger, Event.FTP_UPLOAD);
  }

  @Override
  @SneakyThrows
  public void sendContent(ReportDataResponse reportDataResponse,
      ScheduleInfo scheduleInfo) {
    FTPDelivery ftpDelivery = (FTPDelivery) scheduleInfo.getDeliveryData();
    String fileName = getFileName(scheduleInfo.getReportName());
    EventLog.EventLogBuilder ftpUploadEventBuilder = subscriptionEventLogger
        .initiateEventLogBuilder(scheduleInfo, event);
    subscriptionEventLogger.startEventAudit(ftpUploadEventBuilder.build());
    Object client = loginFTPClient(ftpDelivery, scheduleInfo);
    try {
      writeResponseToFTP(ftpDelivery, fileName, scheduleInfo, reportDataResponse,
          ftpUploadEventBuilder, client);
    } catch (Exception e) {
      handleException(ftpUploadEventBuilder, e);
    } finally {
      closeFTPConnection(client);
    }
  }

  private void writeResponseToFTP(FTPDelivery ftpDelivery, String fileName,
      ScheduleInfo scheduleInfo, ReportDataResponse reportDataResponse,
      EventLogBuilder ftpUploadEventBuilder, Object client) throws IOException, URISyntaxException {
    String fileNameWithoutExtension = ftpDelivery.getFtpLocation() + fileName;
    ScheduleInfo.DeliveryType deliveryType = scheduleInfo.getDeliveryType();
    switch (deliveryType) {
      case CSV:
        try  {
          String finalFileName = getFileNameByFormat(deliveryType, fileNameWithoutExtension);
          writeToFile(client,scheduleInfo, fileNameWithoutExtension, reportDataResponse);
          renameTempFile(client, fileNameWithoutExtension + tmp_extension, finalFileName,
              scheduleInfo);
          markSuccessful(ftpUploadEventBuilder);
        } catch (Exception e) {
          String errorMessage = MessageFormat.format(
              "Error happened while writing to output stream for schedule id <{0}> : <{1}>",
              scheduleInfo.getSubscriptionId(), e.getMessage());
          log.error(errorMessage, e);
          throw e;
        }
        break;
      case D42:
        throw new ClientSideException("D42 Not supported for FTP");
    }
  }

  protected void renameTempFile(Object client, String tempFileName, String finalFileName,
      ScheduleInfo scheduleInfo)
      throws IOException {
    FTPClient ftpClient = (FTPClient) client;
    ftpClient.completePendingCommand();
    log.debug(MessageFormat
        .format("Successfully executed CompletePendingCommand for scheduleId <{0}>",
            scheduleInfo.getSubscriptionId()));
    ftpClient.rename(tempFileName, finalFileName);
    log.info(MessageFormat.format("tmp file successfully renamed for scheduleId <{0}>",
        scheduleInfo.getSubscriptionId()));
  }

  protected void closeFTPConnection(Object client) {
    FTPClient ftpClient = (FTPClient) client;
    try {
      ftpClient.logout();
      if (ftpClient.isConnected()) {
        ftpClient.disconnect();
      }
    } catch (Exception e) {
      log.warn(e.getMessage(), e);
    }
  }

  private void handleException(EventLogBuilder ftpUploadEventBuilder, Exception e) {
    ftpUploadEventBuilder.completedAt(new Date()).message(e.getMessage())
        .state(EventLog.State.FAILED);
    subscriptionEventLogger.updateEventLogAudit(ftpUploadEventBuilder.build());
    failureMeter.mark();
    throw new ClientSideException(e);
  }

  private void markSuccessful(EventLog.EventLogBuilder ftpUploadEventBuilder) {
    successMeter.mark();
    ftpUploadEventBuilder.completedAt(new Date()).state(EventLog.State.COMPLETED);
    subscriptionEventLogger.updateEventLogAudit(ftpUploadEventBuilder.build());
  }

  @Override
  public void sendFailureContent(ScheduleInfo scheduleInfo, Exception e, List<String> subscribers) {
    return;
  }

  @Override
  public void sendSubscriptionExpiryComm(ScheduleInfo scheduleInfo, List<String> subscribers, Date expiryDate) {
    return;
  }

  protected OutputStream initializeOutputStream(Object client, ScheduleInfo scheduleInfo,
      String tmpFileName) throws IOException {
    FTPClient ftpClient = (FTPClient) client;

    OutputStream outputStream = ftpClient.storeFileStream(tmpFileName);
    if (outputStream == null) {
        throw new IllegalStateException(ftpClient.getReplyString());
    }
    if (FTPReply.isPositiveIntermediate(ftpClient.getReplyCode())) {
      throw new ClientSideException(MessageFormat.format(
            "FTP upload Failed: Problem Writing to file. Reply Code :<{0}> for scheduleId <{1}>",
              ftpClient.getReplyCode(), scheduleInfo.getSubscriptionId()));
    }
    return outputStream;
  }

  public void writeToFile(Object client, ScheduleInfo scheduleInfo, String fileNameWithoutExtension,
                           ReportDataResponse reportDataResponse) throws IOException, URISyntaxException {
      RawQueryResultWithSchema queryResult = reportDataResponse.getQueryCachedResult()
              .getQueryResult();
      try(OutputStream outputStream = initializeOutputStream(client, scheduleInfo,
              fileNameWithoutExtension + tmp_extension);) {
        if (reportDataResponse.getQueryCachedResult().isTruncated()) {
          writeFromD42(reportDataResponse, outputStream);
        } else {
          writeFromResponse(queryResult, outputStream);
        }
      }

  }

  private void writeFromResponse(RawQueryResultWithSchema queryResult, OutputStream outputStream)
      throws IOException {
    List<String> headers = queryResult.getSchema().columns.stream()
            .filter(SelectColumn::isVisible).map(SelectColumn::getAlias).collect(
                    Collectors.toList());
    try (CSVStream csvStream = new CSVStream(queryResult.getData().iterator(), headers)) {
      Iterator<byte[]> csvByteIterator = Iterators
              .transform(csvStream.iterator(), String::getBytes);
      while (csvByteIterator.hasNext()) {
        outputStream.write(csvByteIterator.next());
        outputStream.write(csvStream.getRowSeperator().getBytes());
      }
    }
  }

  private void writeFromD42(ReportDataResponse reportDataResponse, OutputStream outputStream)
      throws URISyntaxException, IOException {
    AmazonS3 conn = d42Client.getD42Connection();
    S3Object s3Object = conn.getObject(d42Client.getBucket(),
            fetchD42KeyForReportData(reportDataResponse));
    try (InputStream s3ObjectInputStream = s3Object.getObjectContent()) {
      IOUtils.copy(s3ObjectInputStream, outputStream, 2048);
    }
  }


  protected String getMetricsKeyForSuccess() {
    return StringUtils.join(
        Arrays.asList("subscription", "ftp", "success"), '.');
  }

  protected String getMetricsKeyForFailure() {
    return StringUtils.join(Arrays.asList("subscription", "ftp", "failure"), '.');
  }

  private String getFileName(String fileName) {
    return (fileName + " " + getCurrentTimeStamp()).replaceAll(" ", "_");
  }

  private static String getCurrentTimeStamp() {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss");
    return formatter.format(LocalDateTime.now());
  }

  private String getFileNameByFormat(ScheduleInfo.DeliveryType deliveryType,
      String fileNameWithoutExtn) {
    return deliveryType == ScheduleInfo.DeliveryType.CSV ? fileNameWithoutExtn + ".csv" :
        fileNameWithoutExtn + ".zip";
  }

  public String fetchD42KeyForReportData(ReportDataResponse reportDataResponse) throws URISyntaxException {
    String path = new URI(reportDataResponse.getQueryCachedResult().getD42Link()).getPath();
    return path.substring(path.lastIndexOf('/') + 1);
  }

  public Object loginFTPClient(FTPDelivery ftpDelivery, ScheduleInfo scheduleInfo) {
    FTPClient ftpClient = new FTPClient();
    try {
      ftpClient.connect(ftpDelivery.getHost());
      if (ftpClient.login(ftpDelivery.getUserName(), ftpDelivery.getPassword())) {
        log.info(MessageFormat
                .format("FTP login successful for scheduleId <{0}>", scheduleInfo.getSubscriptionId()));
        ftpClient.enterLocalPassiveMode();
        return ftpClient;
      }
      throw new RuntimeException("Invalid Credentials");
    }
    catch (Exception e) {
      closeFTPConnection(ftpClient);
      String errorMessage = MessageFormat
              .format("FTP upload Failed: Unable to login due to Reason <{0}> , Response code :  <{1}> for scheduleId <{2}>",
                      e.getMessage(), ftpClient.getReplyCode(), scheduleInfo.getSubscriptionId());
      log.error(errorMessage);
      throw new ClientSideException(errorMessage);
    }
  }
}