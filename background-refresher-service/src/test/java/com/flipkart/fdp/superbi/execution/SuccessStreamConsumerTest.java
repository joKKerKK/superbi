package com.flipkart.fdp.superbi.execution;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ServerSideTransformer;
import com.flipkart.fdp.superbi.cosmos.transformer.CosmosServerTransformer;
import com.flipkart.fdp.superbi.d42.D42Uploader;
import com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.gcs.GcsUploader;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.InMemoryCacheDao;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.config.D42MetaConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.collect.Lists;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class SuccessStreamConsumerTest {

  private final CacheDao cacheDao = new InMemoryCacheDao(
      1000,
      30,
      TimeUnit.DAYS);

  @Mock
  private LockDao lockDao;

  @Mock
  D42Uploader d42Uploader;
  @Mock
  GcsUploader gcsUploader;

  private MetricRegistry metricRegistry = new MetricRegistry();

  @Mock
  D42MetaConfig d42MetaConfig;

  @Mock
  ServerSideTransformer serverSideTransformer;

  private static final BackgroundRefresherConfig BACKGROUND_REFRESHER_CONFIG = new BackgroundRefresherConfig(
      2, 3, 4, 5, 6, 7, 7, 8, 2, 0.0d
      , 1024);


  private DSQuery getSampleDSQuery() {
    List<SelectColumn> selectColumns = Lists.newArrayList(
        ExprFactory.SEL_COL("event_name").as("SALE_NAME").selectColumn,
        new SelectColumn.SimpleColumn("business_unit", "BU"),
        ExprFactory.AGGR("gmv", AggregationType.SUM).as("GMV").selectColumn
    );

    return DSQuery.builder()
        .withColumns(selectColumns)
        .withFrom("test_gmv_unit_target_fact")
        .withGroupByColumns(Lists.newArrayList("event_name", "business_unit"))
        .withLimit(1)
        .build();
  }

  private QueryPayload createQueryPayload(DSQuery dsQuery) {
    MetaDataPayload metaDataPayload = new MetaDataPayload("test", "test", "test", "test", new HashMap<>());
    return new QueryPayload("VERTICA_DEFAULT",
        "attemptKey", "cacheKey",  0, null, 0, "", "", "", dsQuery, new HashMap<>(), new HashMap<>()
        , metaDataPayload
    );
  }

  private QueryPayload createNativeQueryPayload(Object nativeQuery) {
    // Pass DSQuery as null
    MetaDataPayload metaDataPayload = new MetaDataPayload("test", "test", "test", "test", new HashMap<>());
    return new QueryPayload("VERTICA_DEFAULT",
            "attemptKey", "cacheKey", 0, new NativeQuery(nativeQuery), 0, "", "", "", null, new HashMap<>(), new HashMap<>()
            , metaDataPayload
    );
  }

   private static List<String> columnNames = Lists.newArrayList("SALE_NAME", "BU", "GMV");

  @Before
  public void setUp() throws Exception {
    JSScriptEngineAccessor.initScriptEngine();
  }

  /*
  Each result row gets transformed and nothing is left
   */
  @Test
  @SneakyThrows
  public void testEachRowWasTransformedWhenNotTruncated() {
    final DSQuery dsQuery = getSampleDSQuery();
    QueryPayload queryPayload = createQueryPayload(dsQuery);

    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(queryPayload)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    List<List<Object>> rows = Lists.<List<Object>>newArrayList(
        Lists.newArrayList("base_sale", "business_unit", 100),
        Lists.newArrayList("base_sale", "business_unit_1", 150)
    );

    QueryResult queryResult = new QueryResult() {
      private final Iterator<List<Object>> iterator = rows.iterator();

      @Override
      public Iterator<List<Object>> iterator() {
        return iterator;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };

    SuccessStreamConsumer successStreamConsumer = new SuccessStreamConsumer(cacheDao, d42Uploader
        ,gcsUploader, metricRegistry, d42MetaConfig, (originalResult, dsQuery1, params, timer) -> {
      CosmosServerTransformer cosmosServerTransformer = new CosmosServerTransformer(queryResult,
          dsQuery1, new HashMap<>(), new Timer(), serverSideTransformer);
      return cosmosServerTransformer.postProcess();
    });

    // Invoke SuccessStreamConsumer with QueryResult
    successStreamConsumer.accept(task, queryResult);

    // Verify each row was passed through transformer
    verify(serverSideTransformer, times(rows.size())).postProcessSingleRow(Mockito.any(), Mockito.any());
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTruncatedRows(), 2);
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTotalNumberOfRows(), rows.size());
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getQueryResult().getColumns(), queryResult.getColumns());
  }

  /*
Each result row gets transformed and nothing is left
 */
  @Test
  @SneakyThrows
  public void testEachRowWasTransformedWhenNotTruncatedForNativeQuery() {
    Object sqlNativeQuery = "select event_name as SALE_NAME, business_unit as BU, gmv as GMV from temp_table";
    QueryPayload queryPayload = createNativeQueryPayload(sqlNativeQuery);

    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(queryPayload)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    List<List<Object>> rows = Lists.<List<Object>>newArrayList(
        Lists.newArrayList("base_sale", "business_unit", 100),
        Lists.newArrayList("base_sale", "business_unit_1", 150)
    );

    QueryResult queryResult = new QueryResult() {
      private final Iterator<List<Object>> iterator = rows.iterator();

      @Override
      public Iterator<List<Object>> iterator() {
        return iterator;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };

    SuccessStreamConsumer successStreamConsumer = new SuccessStreamConsumer(cacheDao, d42Uploader
        ,gcsUploader, metricRegistry, d42MetaConfig, (originalResult, dsQuery1, params, timer) -> {
      CosmosServerTransformer cosmosServerTransformer = new CosmosServerTransformer(queryResult,
          dsQuery1, new HashMap<>(), new Timer(), serverSideTransformer);
      return cosmosServerTransformer.postProcess();
    });

    // Invoke SuccessStreamConsumer with QueryResult
    successStreamConsumer.accept(task, queryResult);

    // Verify each row was passed through transformer
    verify(serverSideTransformer, times(0));
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTruncatedRows(), 2);
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTotalNumberOfRows(), rows.size());
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getQueryResult().getColumns(), queryResult.getColumns());
  }


  /*
  Result gets truncated upto 1000 rows
   */
  @Test
  @SneakyThrows
  public void testLimitedRowsWereTransformedWhenTruncated() {
    QueryPayload queryPayload = createQueryPayload(getSampleDSQuery());

    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(queryPayload)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    List<List<Object>> rows = Lists.newArrayList();

    for (int i=0; i<2000; i++){
      rows.add(Lists.newArrayList("base_sale", "business_unit_"+i, 100+i));
    }

    QueryResult queryResult = new QueryResult() {
      private final Iterator<List<Object>> iterator = rows.iterator();

      @Override
      public Iterator<List<Object>> iterator() {
        return iterator;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };
    Mockito.when(d42MetaConfig.getD42UploadClients()).thenReturn(Lists.newArrayList());
    SuccessStreamConsumer successStreamConsumer = new SuccessStreamConsumer(cacheDao, d42Uploader
        ,gcsUploader, metricRegistry, d42MetaConfig, (originalResult, dsQuery1, params, timer) -> {
      CosmosServerTransformer cosmosServerTransformer = new CosmosServerTransformer(queryResult,
          dsQuery1, new HashMap<>(), new Timer(), serverSideTransformer);
      return cosmosServerTransformer.postProcess();
    });
    successStreamConsumer.accept(task, queryResult);
    // Verify invocations for serverSideTransformer for truncated rows i.e. 1000
    verify(serverSideTransformer, times(1000)).postProcessSingleRow(Mockito.any(), Mockito.any());
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTruncatedRows(), 1000);
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTotalNumberOfRows(), rows.size());
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getQueryResult().getColumns(), queryResult.getColumns());
  }

  /*
Result gets truncated upto 1000 rows
 */
  @Test
  @SneakyThrows
  public void testLimitedRowsWereTransformedWhenTruncatedForNativeQuery() {
    Object sqlNativeQuery = "select event_name as SALE_NAME, business_unit as BU, gmv as GMV from temp_table";
    QueryPayload queryPayload = createNativeQueryPayload(sqlNativeQuery);

    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
            .lockDao(lockDao)
            .queryPayload(queryPayload)
            .remainingRetry(0)
            .executionAfterTimestamp(new Date().getTime())
            .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    List<List<Object>> rows = Lists.newArrayList();

    for (int i=0; i<2000; i++){
      rows.add(Lists.newArrayList("base_sale", "business_unit_"+i, 100+i));
    }

    QueryResult queryResult = new QueryResult() {
      private final Iterator<List<Object>> iterator = rows.iterator();

      @Override
      public Iterator<List<Object>> iterator() {
        return iterator;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };
    Mockito.when(d42MetaConfig.getD42UploadClients()).thenReturn(Lists.newArrayList());
    SuccessStreamConsumer successStreamConsumer = new SuccessStreamConsumer(cacheDao, d42Uploader
            ,gcsUploader, metricRegistry, d42MetaConfig, (originalResult, dsQuery1, params, timer) -> {
      CosmosServerTransformer cosmosServerTransformer = new CosmosServerTransformer(queryResult,
              dsQuery1, new HashMap<>(), new Timer(), serverSideTransformer);
      return cosmosServerTransformer.postProcess();
    });
    successStreamConsumer.accept(task, queryResult);
    // Verify invocations for serverSideTransformer for truncated rows i.e. 1000
    verify(serverSideTransformer, times(0));
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTruncatedRows(), 1000);
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getTotalNumberOfRows(), rows.size());
    Assert.assertEquals(cacheDao.get("cacheKey", QueryResultCachedValue.class).get().getQueryResult().getColumns(), queryResult.getColumns());
  }

  @Test
  @SneakyThrows
  public void testQueryResultAutoclose() {
    QueryPayload queryPayload = createQueryPayload(getSampleDSQuery());

    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
            .lockDao(lockDao)
            .queryPayload(queryPayload)
            .remainingRetry(0)
            .executionAfterTimestamp(new Date().getTime())
            .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    List<List<Object>> rows = Lists.newArrayList();

    for (int i=0; i<2000; i++){
      rows.add(Lists.newArrayList("base_sale", "business_unit_"+i, 100+i));
    }

    final Iterator<List<Object>> iterator = rows.iterator();
    QueryResult queryResult = Mockito.mock(QueryResult.class);

    Mockito.when(queryResult.iterator()).thenReturn(iterator);

    SuccessStreamConsumer successStreamConsumer = new SuccessStreamConsumer(cacheDao, d42Uploader
            ,gcsUploader, metricRegistry, d42MetaConfig, (originalResult, dsQuery1, params, timer) -> {
      CosmosServerTransformer cosmosServerTransformer = new CosmosServerTransformer(queryResult,
              dsQuery1, new HashMap<>(), new Timer(), serverSideTransformer);
      return cosmosServerTransformer.postProcess();
    });
    successStreamConsumer.accept(task, queryResult);

    // Verify if stream is close
    verify(queryResult, times(1)).close();
  }

  @Test
  public void handleNativeQuerySuccessfulExecution() {
    Object sqlNativeQuery = "select event_name as SALE_NAME, business_unit as BU, gmv as GMV from temp_table";
    QueryPayload queryPayload = createNativeQueryPayload(sqlNativeQuery);

    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
            .lockDao(lockDao)
            .queryPayload(queryPayload)
            .remainingRetry(0)
            .executionAfterTimestamp(new Date().getTime())
            .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    List<List<Object>> rows = Lists.newArrayList();

    for (int i=0; i<2000; i++){
      rows.add(Lists.newArrayList("base_sale", "business_unit_"+i, 100+i));
    }

    final Iterator<List<Object>> iterator = rows.iterator();
    QueryResult queryResult = Mockito.mock(QueryResult.class);

    Mockito.when(queryResult.iterator()).thenReturn(iterator);
    Mockito.when(queryResult.getColumns()).thenReturn(columnNames);

    SuccessStreamConsumer successStreamConsumer = new SuccessStreamConsumer(cacheDao, d42Uploader
            ,gcsUploader, metricRegistry, d42MetaConfig, (originalResult, dsQuery1, params, timer) -> {
      CosmosServerTransformer cosmosServerTransformer = new CosmosServerTransformer(queryResult,
              dsQuery1, new HashMap<>(), new Timer(), serverSideTransformer);
      return cosmosServerTransformer.postProcess();
    });
    successStreamConsumer.accept(task, queryResult);

    // Verify if stream is close
    verify(queryResult, times(1)).close();
  }
}
