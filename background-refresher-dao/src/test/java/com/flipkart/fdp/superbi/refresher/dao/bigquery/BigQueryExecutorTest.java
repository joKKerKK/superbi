package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.InMemoryCacheDao;
import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.executor.BigQueryExecutor;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.executor.BigQueryExecutorImpl;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;


public class BigQueryExecutorTest {

  @Mock
  private GringottsClient gringottsClient;

  @Mock
  private MetaDataPayload metaDataPayload;

  @Mock
  private MetricRegistry metricRegistry;

  private BigQuery bigQuery;
  private String query =
      "WITH latest AS (\n"
          + "SELECT *, RANK() OVER (\n"
          + "    PARTITION BY order_id\n"
          + "      ORDER BY updated_at DESC\n"
          + "  )\n"
          + "  as rank FROM `fk-sanbox-fdp-temp-1.akshay_dataset_1.mansi_streaming_data`\n"
          + "  where unit_creation_timestamp >= TIMESTAMP('2021-11-14 23:59:59')\n"
          + ")\n"
          + "\n"
          + "SELECT sum(net_amount)\n"
          + "   FROM latest\n"
          + "   WHERE rank=1";

  @Before
  @SneakyThrows
  public void setup() {
    Map<String, String> attributes = new HashMap<>();
    String clientId = attributes.get("clientId");
    String clientEmail = attributes.get("clientEmail");
    String pKey = attributes.get("pKey");
    String privateKeyId = attributes.get("privateKeyId");
    String projectId = attributes.get("projectId");

    Credentials credentials = ServiceAccountCredentials.fromPkcs8
        (clientId, clientEmail, pKey, privateKeyId, null);

    bigQuery = BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials)
        .build().getService();

  }

  @Test
  @Ignore
  public void testBigQueryExecution() {
    CacheDao jobStore = new InMemoryCacheDao(100, 200, TimeUnit.SECONDS);
    BigQueryJobConfig bigQueryJobConfig = BigQueryJobConfig.builder().totalTimeoutLimitMs(100L)
        .build();
    BigQueryExecutor bigQueryExecutor = new BigQueryExecutorImpl(jobStore, bigQueryJobConfig,
        bigQuery);
    BigQueryDataSourceDao bigQueryDataSourceDao = new BigQueryDataSourceDao(bigQueryExecutor,
        gringottsClient, metricRegistry);
    DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(query).
        cacheKey("cacheKey1").metaDataPayload(metaDataPayload).build();
    QueryResult queryResult = bigQueryDataSourceDao.getStream(dataSourceQuery);
    Iterator<List<Object>> iterator = queryResult.iterator();
  }

}
