package com.flipkart.fdp.superbi.subscription.executors;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.subscription.delivery.FtpExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.model.QueryResultCachedValue;
import com.flipkart.fdp.superbi.subscription.model.RawQueryResultWithSchema;
import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo.DeliveryType;
import com.google.common.collect.ImmutableList;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(IOUtils.class)
public class FTPExecutorWriteToFileTest {
  @Mock
  private FTPClient ftpClient;
  @Mock
  private D42Client d42Client;
  @Mock
  private Meter successMeter;
  @Mock
  private Meter failureMeter;
  @Mock
  private SubscriptionEventLogger subscriptionEventLogger;
  @Mock
  private MetricRegistry metricRegistry;
  @Mock
  private ScheduleInfo scheduleInfo;
  @Mock
  private OutputStream outputStream;
  @Mock
  private AmazonS3 amazonS3;
  @Mock
  private S3Object s3Object;
  @Mock
  private S3ObjectInputStream s3ObjectInputStream;
  private FtpExecutor ftpExecutor;

  @Before
  public void setUp() {
    ftpExecutor = new FtpExecutor(d42Client, metricRegistry, subscriptionEventLogger);
  }

  @Test
  public void testWriteFromD42() throws Exception {
    Mockito.when(ftpClient.storeFileStream(Mockito.anyString())).thenReturn(outputStream);
    Mockito.when(ftpClient.getReplyCode()).thenReturn(200);
    Mockito.when(d42Client.getD42Connection()).thenReturn(amazonS3);
    Mockito.when(amazonS3.getObject(Mockito.anyString(), Mockito.any())).thenReturn(s3Object);
    Mockito.when(s3Object.getObjectContent()).thenReturn(s3ObjectInputStream);
    PowerMockito.mockStatic(IOUtils.class);
    Mockito.when(IOUtils.copy(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(0l);
    QueryResultCachedValue queryResultCachedValue = getQueryResultCachedValueBuilder()
        .truncated(true).d42Link(
            "http://10.47.2.22/reports/superbi_Vertica_DEFAULT_013dd55b12154af421255c3f8f984dc01608730556426.csv?AWSAccessKeyId=WFT11C502A49DY&Expires=1609335358&Signature=EgrTA0ZC8N5Al")
        .build();
    ReportDataResponse reportDataResponse = ReportDataResponse.builder()
        .queryCachedResult(queryResultCachedValue).build();
    ftpExecutor
        .writeToFile(ftpClient, scheduleInfo, "test_file", reportDataResponse);
    Mockito.verify(d42Client, Mockito.times(1)).getD42Connection();
  }

  @Test
  public void testWriteFromResponse() throws Exception {
    Mockito.when(ftpClient.storeFileStream(Mockito.anyString())).thenReturn(outputStream);
    Mockito.when(ftpClient.getReplyCode()).thenReturn(200);
    QueryResultCachedValue queryResultCachedValue = getQueryResultCachedValueBuilder()
        .truncated(false).build();
    ReportDataResponse reportDataResponse = ReportDataResponse.builder()
        .queryCachedResult(queryResultCachedValue).build();
    ftpExecutor
        .writeToFile(ftpClient, scheduleInfo, "test_file", reportDataResponse);
    Mockito.verify(outputStream, Mockito.atLeastOnce()).write(Mockito.any());
  }

  @Test
  public void testFetchD42KeyForReportData() throws Exception {
    QueryResultCachedValue queryResultCachedValue = getQueryResultCachedValueBuilder()
        .truncated(true).d42Link(
            "http://10.47.2.22/reports/superbi_Vertica_DEFAULT_013dd55b12154af421255c3f8f984dc01608730556426.csv?AWSAccessKeyId=WFT11C502A49DExpires=1609335358&Signature=EgrTA/0ZC8N5Alj")
        .build();
    ReportDataResponse reportDataResponse = ReportDataResponse.builder()
        .queryCachedResult(queryResultCachedValue).build();
    Assert.assertEquals("superbi_Vertica_DEFAULT_013dd55b12154af421255c3f8f984dc01608730556426.csv",
        ftpExecutor.fetchD42KeyForReportData(reportDataResponse));
  }

  private QueryResultCachedValue.QueryResultCachedValueBuilder getQueryResultCachedValueBuilder() {
    Schema schema = new Schema(ImmutableList.copyOf(new ArrayList<SelectColumn>()), null, null, 10);
    RawQueryResultWithSchema rawQueryResult = new RawQueryResultWithSchema(new ArrayList<>(),
        new HashMap<>(), schema);
    QueryResultCachedValue queryResultCached = new QueryResultCachedValue("", 0l, "",
        rawQueryResult,
        300, 300, false, "", "");
    return QueryResultCachedValue.builder().cachedAtTime(0l).queryResult(rawQueryResult);
  }

}
