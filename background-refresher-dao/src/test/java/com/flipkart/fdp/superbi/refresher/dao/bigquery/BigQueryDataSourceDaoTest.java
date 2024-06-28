package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.exceptions.SuperBiException;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.InMemoryCacheDao;
import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.executor.BigQueryExecutor;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.executor.BigQueryExecutorImpl;
import com.flipkart.fdp.superbi.refresher.dao.exceptions.DataSizeLimitExceedException;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.io.BaseEncoding;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({QueryJobConfiguration.class, TableId.class})
public class BigQueryDataSourceDaoTest {

  @Mock
  private BigQuery bigquery;

  @Mock
  private Job queryJob;

  @Mock
  private TableResult tableResult;

  @Mock
  private JobStatus jobStatus;

  @Mock
  private JobId jobId;

  @Mock
  private TableId tableId;

  @Mock
  private QueryJobConfiguration jobConfiguration;

  @Mock
  private Table table;

  @Mock
  private Long numBytes;

  @Mock
  private Iterable<FieldValueList> iterable;

  @Mock
  private GringottsClient gringottsClient;

  @Mock
  private MetaDataPayload metaDataPayload;

  private static Iterator<FieldValueList> iterator;

  @Mock
  private Iterator<FieldValueList> mockedIterator;

  @Mock
  private MetricRegistry metricRegistry;

  private BigQueryExecutor bigQueryExecutor;

  private static Schema schema;

  private BigQueryDataSourceDao bigQueryDataSourceDao;

  private String query = "SELECT *, RANK() OVER (\n"
      + "    PARTITION BY order_id\n"
      + "      ORDER BY updated_at DESC\n"
      + "  )\n"
      + "  as rank FROM `fk-sanbox-fdp-temp-1.akshay_dataset_1.reporting_clustering_only`\n"
      + "  where unit_creation_timestamp >= TIMESTAMP('2021-11-14 23:59:59')\n"
      + ")\n"
      + "\n"
      + "SELECT *\n"
      + "   FROM latest\n"
      + "   WHERE rank=1";

  private static final List<List<Object>> rowList = Arrays
      .asList(Arrays.asList("AndroidApp", new Long(13982297)), Arrays.asList("Website", new Long(166712)));

  @Before
  public void setup() {
    BigQueryJobConfig bigQueryJobConfig = BigQueryJobConfig.builder()
        .totalTimeoutLimitMs(new Long(-1)).tableSizeLimitInMbs(new Long(1)).build();
    CacheDao jobStore = new InMemoryCacheDao(100, 200, TimeUnit.SECONDS);
    bigQueryExecutor = new BigQueryExecutorImpl(jobStore, bigQueryJobConfig,
        bigquery);
    bigQueryDataSourceDao = new BigQueryDataSourceDao(bigQueryExecutor, gringottsClient,
        metricRegistry);
    FieldValue fieldValue1 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AndroidApp");
    FieldValue fieldValue2 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "13982297");
    FieldValue fieldValue3 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Website");
    FieldValue fieldValue4 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "166712");
    Field column1 = Field.of("platform", LegacySQLTypeName.STRING);
    Field column2 = Field.of("value", LegacySQLTypeName.INTEGER);
    schema = Schema.of(column1, column2);

    FieldValueList fieldValuesRow1 = FieldValueList.of(Arrays.asList(fieldValue1, fieldValue2));
    FieldValueList fieldValuesRow2 = FieldValueList.of(Arrays.asList(fieldValue3, fieldValue4));

    List<FieldValueList> fieldValueLists = Arrays.asList(fieldValuesRow1, fieldValuesRow2);
    iterator = fieldValueLists.iterator();

    Map<String,String> labels = new HashMap<>();
    labels.put("org","bigfoot");
    labels.put("namespace", "test");
    labels.put("billing_app_id","bigfoot__test");
    Mockito.when(gringottsClient.getBillingLabels("user")).thenReturn(labels);
    Mockito.when(metaDataPayload.getUsername()).thenReturn("user");
  }

  @Test
  public void testBigQueryNewSuccessfulQuery() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(numBytes);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);
    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);
    Mockito.when(iterable.iterator()).thenReturn(iterator);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    List<Object> row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(0).get(0));
    Assert.assertEquals(row.get(1), rowList.get(0).get(1));

    Assert.assertTrue(iterator.hasNext());
    row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(1).get(0));
    Assert.assertEquals(row.get(1), rowList.get(1).get(1));
  }

  @Test(expected = ServerSideException.class)
  public void testBigQueryExceptionDuringResultFetch() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(numBytes);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);
    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);
    Mockito.when(iterable.iterator()).thenReturn(mockedIterator);
    Mockito.doThrow(new BigQueryException(0, "Connection reset")).when(mockedIterator).hasNext();

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
    iterator.hasNext();
  }

  @Test(expected = DataSizeLimitExceedException.class)
  public void testBigQueryNewSuccessfulQueryButLimitBreached() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(2*1024*1024l);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);
    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);
    Mockito.when(iterable.iterator()).thenReturn(iterator);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    List<Object> row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(0).get(0));
    Assert.assertEquals(row.get(1), rowList.get(0).get(1));

    Assert.assertTrue(iterator.hasNext());
    row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(1).get(0));
    Assert.assertEquals(row.get(1), rowList.get(1).get(1));
  }

  @Test(expected = ServerSideException.class)
  public void testBigQueryNewFailQuery() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    BigQueryError error = new BigQueryError("internalError", "",
        "An internal error occured within BigQuery.");
    BigQueryException bigQueryException = new BigQueryException(500,
        "An internal error occured within BigQuery.", error);
    Mockito.when(queryJob.waitFor()).thenThrow(bigQueryException);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(jobStatus.getError()).thenReturn(error);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    bigQueryDataSourceDao.getStream(dataSourceQuery);
  }

  @Test(expected = ServerSideException.class)
  public void testJobCreationFailure() throws InterruptedException {
    BigQueryError error = new BigQueryError("internalError", "",
        "An internal error occured within BigQuery.");
    BigQueryException bigQueryException = new BigQueryException(500,
        "An internal error occured within BigQuery.", error);
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenThrow(bigQueryException);
    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    bigQueryDataSourceDao.getStream(dataSourceQuery);
  }

  @Test
  public void testBigQueryLatchSuccessful() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(bigquery.getJob(Mockito.any(JobId.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(numBytes);
    Mockito.when(jobStatus.getState()).thenReturn(JobStatus.State.RUNNING);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);

    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Mockito.when(iterable.iterator()).thenReturn(iterator);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);
    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey2").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    List<Object> row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(0).get(0));
    Assert.assertEquals(row.get(1), rowList.get(0).get(1));

    Assert.assertTrue(iterator.hasNext());
    row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(1).get(0));
    Assert.assertEquals(row.get(1), rowList.get(1).get(1));
  }

  @Test
  public void testBigQueryLatchFailed() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(bigquery.getJob(Mockito.any(JobId.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(jobStatus.getState()).thenReturn(JobStatus.State.DONE);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(numBytes);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);
    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Mockito.when(iterable.iterator()).thenReturn(iterator);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey2").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    List<Object> row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(0).get(0));
    Assert.assertEquals(row.get(1), rowList.get(0).get(1));

    Assert.assertTrue(iterator.hasNext());
    row = iterator.next();
    Assert.assertEquals(row.get(0), rowList.get(1).get(0));
    Assert.assertEquals(row.get(1), rowList.get(1).get(1));
  }

  @Test
  public void testBigQuerySchema() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(numBytes);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);
    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Mockito.when(iterable.iterator()).thenReturn(iterator);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    List<String> columnList = queryResult.getColumns();
    Assert.assertEquals(columnList, Arrays.asList("platform", "value"));
  }

  @SneakyThrows
  @Test(expected = ClientSideException.class)
  public void testWhenJobTimeout() {
    BigQueryJobConfig bigQueryJobConfig = BigQueryJobConfig.builder().totalTimeoutLimitMs(1000L)
        .build();
    CacheDao jobStore = new InMemoryCacheDao(100, 200, TimeUnit.SECONDS);
    bigQueryExecutor = new BigQueryExecutorImpl(jobStore, bigQueryJobConfig, bigquery);
    BigQueryDataSourceDao bigQueryDataSourceDao = new BigQueryDataSourceDao(bigQueryExecutor,
        gringottsClient, metricRegistry);
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    BigQueryError error = new BigQueryError("stopped", "",
        "Job execution was cancelled: Job timed out after 1s");
    BigQueryException bigQueryException = new BigQueryException(499,
        "Job execution was cancelled: Job timed out after 1s", error);
    Mockito.when(queryJob.waitFor()).thenThrow(bigQueryException);
    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    bigQueryDataSourceDao.getStream(dataSourceQuery);
  }

  @Test(expected = SuperBiException.class)
  public void testBigQueryForSuperBiException() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenThrow(SuperBiException.class);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    bigQueryDataSourceDao.getStream(dataSourceQuery);
  }

  @Test
  public void testBigQueryConverter() throws InterruptedException {
    Mockito.when(bigquery.create(Mockito.any(JobInfo.class))).thenReturn(queryJob);
    Mockito.when(queryJob.waitFor()).thenReturn(queryJob);
    Mockito.when(queryJob.getStatus()).thenReturn(jobStatus);
    Mockito.when(queryJob.getJobId()).thenReturn(jobId);
    Mockito.when(queryJob.getQueryResults()).thenReturn(tableResult);
    Mockito.when(queryJob.getConfiguration()).thenReturn(jobConfiguration);
    Mockito.when(jobConfiguration.getDestinationTable()).thenReturn(tableId);
    Mockito.when(bigquery.getTable(Mockito.any(TableId.class))).thenReturn(table);
    Mockito.when(table.getNumBytes()).thenReturn(numBytes);
    Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
    Field field1 = Field.of("net_gmv", LegacySQLTypeName.INTEGER);
    Field field2 = Field.of("business_unit", LegacySQLTypeName.STRING);
    Field field3 = Field.of("net_bytes", LegacySQLTypeName.BYTES);
    Field field4 = Field.of("net_amount", LegacySQLTypeName.FLOAT);
    Field field5 = Field.of("offer_applied", LegacySQLTypeName.BOOLEAN);
    Field field6 = Field.of("cost_price", LegacySQLTypeName.NUMERIC);
    Field field7 = Field.of("sell_price", LegacySQLTypeName.BIGNUMERIC);
    Field field8 = Field.of("unit_creation_timestamp", LegacySQLTypeName.TIMESTAMP);
    Field field9 = Field.of("order_date", LegacySQLTypeName.DATE);
    Field field10 = Field.of("order_date_time", LegacySQLTypeName.DATETIME);
    Field field11 = Field.of("order_time", LegacySQLTypeName.TIME);
    Field field12 = Field.of("geography", LegacySQLTypeName.GEOGRAPHY);
    Field field13 = Field.of("curtain_raiser_flag", LegacySQLTypeName.GEOGRAPHY);
    Field field14 = Field.of("words", LegacySQLTypeName.STRING);

    Schema schema = Schema.of( field1, field2, field3, field4, field5,
        field6, field7, field8, field9, field10, field11, field12, field13, field14);
    Mockito.when(tableResult.getSchema()).thenReturn(schema);

    FieldValue fieldValue1 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "60000");
    FieldValue fieldValue2 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "LifeStyle");
    FieldValue fieldValue3 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test");
    FieldValue fieldValue4 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123.3");
    FieldValue fieldValue5 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true");
    FieldValue fieldValue6 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123");
    FieldValue fieldValue7 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "12345678");
    FieldValue fieldValue8 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1646731536");
    FieldValue fieldValue9 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2021-01-01");
    FieldValue fieldValue10 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2022-03-14T13:28:53.983838");
    FieldValue fieldValue11 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10:37:52");
    FieldValue fieldValue12 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "['word1', 'word2', 'word3']");
    FieldValue fieldValue13 = FieldValue.of(FieldValue.Attribute.PRIMITIVE, null);

    List<FieldValue> fieldValuesRepeated = new ArrayList<>();
    fieldValuesRepeated.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "word1"));
    fieldValuesRepeated.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "word2"));
    fieldValuesRepeated.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "word3"));
    FieldValue fieldValue14 = FieldValue.of(FieldValue.Attribute.REPEATED, fieldValuesRepeated);

    FieldValueList fieldValuesRow1 = FieldValueList.of(
        Arrays.asList(fieldValue1, fieldValue2, fieldValue3, fieldValue4, fieldValue5, fieldValue6,
            fieldValue7, fieldValue8, fieldValue9, fieldValue10, fieldValue11, fieldValue12, fieldValue13, fieldValue14));

    List<FieldValueList> fieldValueLists = Arrays.asList(fieldValuesRow1);
    Iterator<FieldValueList> fieldValueListIterator = fieldValueLists.iterator();
    Mockito.when(iterable.iterator()).thenReturn(fieldValueListIterator);

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
    Assert.assertTrue(iterator.hasNext());
    List<Object> row = iterator.next();
    Assert.assertEquals(row.get(0), new Long(60000));
    Assert.assertEquals(row.get(1), "LifeStyle");
    Assert.assertArrayEquals((byte[]) row.get(2), BaseEncoding.base64().decode("test"));
    Assert.assertEquals(row.get(3), 123.3);
    Assert.assertEquals(row.get(4), true);
    Assert.assertEquals(row.get(5), new BigDecimal("123"));
    Assert.assertEquals(row.get(6), new BigDecimal("12345678"));
    Assert.assertEquals(row.get(7), Timestamp.ofTimeMicroseconds(getTimestampValue("1646731536")).toDate());
    Assert.assertEquals(row.get(8), "2021-01-01");
    Assert.assertEquals(row.get(9), new DateTime("2022-03-14T13:28:53.983838"));
    Assert.assertEquals(row.get(10), Time.valueOf("10:37:52"));
    Assert.assertEquals(row.get(11), "['word1', 'word2', 'word3']");
    Assert.assertEquals(row.get(12), null);
    Assert.assertEquals(row.get(13), "word1,word2,word3");
  }

  private long getTimestampValue(String value) {
    BigDecimal secondsWithMicro = new BigDecimal(value);
    BigDecimal scaled = secondsWithMicro.scaleByPowerOfTen(6).setScale(0, RoundingMode.HALF_UP);
    return scaled.longValue();
  }
}
