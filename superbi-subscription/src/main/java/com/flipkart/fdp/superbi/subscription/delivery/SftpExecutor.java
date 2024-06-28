package com.flipkart.fdp.superbi.subscription.delivery;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.model.EventLog;
import com.flipkart.fdp.superbi.subscription.model.EventLog.Event;
import com.flipkart.fdp.superbi.subscription.model.EventLog.EventLogBuilder;
import com.flipkart.fdp.superbi.subscription.model.FTPDelivery;
import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class SftpExecutor extends FtpExecutor {
  public SftpExecutor(D42Client d42Client, MetricRegistry metricRegistry,
      SubscriptionEventLogger subscriptionEventLogger) {
    super(d42Client, metricRegistry, subscriptionEventLogger, Event.SFTP_UPLOAD);
  }

  @Override
  @SneakyThrows
  public OutputStream initializeOutputStream(Object client, ScheduleInfo scheduleInfo,
      String tmpFileName) {
    ChannelSftp channelSftp = (ChannelSftp) client;
    return channelSftp.put(tmpFileName);
  }

  @Override
  @SneakyThrows
  public void renameTempFile(Object client, String tempFileName, String finalFileName,
      ScheduleInfo scheduleInfo) {
    ChannelSftp channelSftp = (ChannelSftp) client;
    log.info(
        MessageFormat.format("Successfully written to tmp_file  for scheduleId <{0}>",
            scheduleInfo.getSubscriptionId()));
    channelSftp.rename(tempFileName, finalFileName);
    log.info(MessageFormat.format("tmp file successfully renamed for scheduleId <{0}>",
        scheduleInfo.getSubscriptionId()));
  }

  @Override
  public void closeFTPConnection(Object client) {
    ChannelSftp channelSftp = (ChannelSftp) client;
    try {
      Session session = channelSftp.getSession();
      if (session != null && session.isConnected()) {
        session.disconnect();
        channelSftp.disconnect();
      }
    } catch (Exception e) {
      log.warn(e.getMessage(), e);
    }
  }

  protected String getMetricsKeyForSuccess() {
    return StringUtils.join(
        Arrays.asList("subscription", "sftp", "success"), '.');
  }

  protected String getMetricsKeyForFailure() {
    return StringUtils.join(Arrays.asList("subscription", "sftp", "failure"), '.');
  }

  @Override
  public Object loginFTPClient(FTPDelivery ftpDelivery, ScheduleInfo scheduleInfo) {
    java.util.Properties config = new java.util.Properties();
    config.put("StrictHostKeyChecking", "no");
    JSch jsch = new JSch();
    Session session = null;
    ChannelSftp channelSftp = null;
    try {
      session = jsch.getSession(ftpDelivery.getUserName(), ftpDelivery.getHost(),
          Integer.parseInt(ftpDelivery.getPort()));
      session.setPassword(ftpDelivery.getPassword());
      session.setConfig(config);
      session.connect();
      channelSftp = (ChannelSftp) session.openChannel("sftp");
      channelSftp.connect();
      log.info(MessageFormat
          .format("SFTP login successful for scheduleId <{0}>", scheduleInfo.getSubscriptionId()));
      return channelSftp;
    } catch (Exception exception) {
      closeFTPConnection(channelSftp);
      String errorMessage = MessageFormat
          .format("SFTP upload Failed: Unable to login. Reason : <{0}> for scheduleId <{1}>",
              exception.getMessage(), scheduleInfo.getSubscriptionId());
      log.error(errorMessage);
      throw new ClientSideException(errorMessage);
    }
  }
}
