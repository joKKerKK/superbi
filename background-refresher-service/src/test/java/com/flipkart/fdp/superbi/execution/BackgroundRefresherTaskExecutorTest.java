package com.flipkart.fdp.superbi.execution;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.collect.Lists;
import com.sun.xml.internal.xsom.impl.scd.Iterators;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import reactor.core.publisher.Mono;

@RunWith(PowerMockRunner.class)
public class BackgroundRefresherTaskExecutorTest {

  @Mock
  private BasicQueryExecutor basicQueryExecutor;

  @Mock
  private ExecutionAuditor executionAuditor;

  @Mock
  private BiConsumer<BackgroundRefreshTask, QueryResult> successStreamConsumer;

  @Mock
  private BiConsumer<BackgroundRefreshTask, Throwable> failureStreamConsumer;

  MetricRegistry metricRegistry = new MetricRegistry();

  @Mock
  private LockDao lockDao;

  private static ExecutorService executorService = Executors.newFixedThreadPool(10);

  private static ExecutionLog.ExecutionLogBuilder executionLogBuilder;

  public static final List<SelectColumn> selectColumns = Lists.newArrayList(
      ExprFactory.SEL_COL("event_name").as("event_name").selectColumn,
      new SelectColumn.SimpleColumn("business_unit", "business_unit"),
      ExprFactory.AGGR("gmv", AggregationType.SUM).as("gmv").selectColumn
  );
  public static final String temp_fact_1 = "test_gmv_unit_target_fact";

  public static DSQuery dsQuery = DSQuery.builder()
      .withColumns(selectColumns)
      .withFrom(temp_fact_1)
      .withGroupByColumns(Lists.newArrayList("event_name", "business_unit"))
      .withLimit(1)
      .build();
  private static final BackgroundRefresherConfig BACKGROUND_REFRESHER_CONFIG = new BackgroundRefresherConfig(
      2, 3, 4, 5, 6, 7, 7, 8, 2, 0.0d
      , 1024);
  private static final QueryPayload SAMPLE_QUERY_PAYLOAD = new QueryPayload("VERTICA_DEFAULT",
      "attemptKey", "cacheKey", 0, new NativeQuery("select * from A"), 0, "", "", "", dsQuery,
      new HashMap<>(), new HashMap<>(), null
  );

  private static final QueryPayload SAMPLE_QUERY_PAYLOAD_FOR_NATIVE_QUERY = createNativeQueryPayload();

  private static QueryPayload createNativeQueryPayload() {
    // Pass dsQuery as null
    String sqlNativeQuery = "select event_name, business_unit, gmv from temp_table";
    MetaDataPayload metaDataPayload = new MetaDataPayload("test", "test", "test", "test", new HashMap<>());
    return new QueryPayload("VERTICA_DEFAULT",
        "attemptKey", "cacheKey", 0, new NativeQuery(sqlNativeQuery), 0, "", "", "", null,
        new HashMap<>(), new HashMap<>()
        , metaDataPayload
    );
  }

  private static List<String> columnNames = Lists.newArrayList("event_name", "business_unit",
      "gmv");

  @Test
  public void whenTaskIsLocked() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();
    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(true);
    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());
    Mockito.doNothing().when(successStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return null;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };

    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenReturn(queryResult);
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    executor.executeTask(task).subscribe();

    verify(successStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
    verify(failureStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
  }

  @Test
  public void whenTaskIsLockedForNativeQueryExecution() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD_FOR_NATIVE_QUERY)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();
    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(true);
    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());
    Mockito.doNothing().when(successStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return null;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };

    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenReturn(queryResult);
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    executor.executeTask(task).subscribe();

    verify(successStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
    verify(failureStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
  }

  @Test
  public void whenTaskNotLockedAndResultReturned() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD)
        .remainingRetry(5)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();
    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(false);
    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());
    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return null;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };
    Mockito.doNothing().when(successStreamConsumer).accept(task, queryResult);
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenReturn(queryResult);
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    executor.executeTask(task).subscribe();

    verify(successStreamConsumer, times(1)).accept(Mockito.any(), Mockito.any());
    verify(failureStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
  }

  @Test
  public void whenTaskNotLockedAndResultReturnedForNativeQueryExecution() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD_FOR_NATIVE_QUERY)
        .remainingRetry(5)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();
    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(false);
    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());
    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return null;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };
    Mockito.doNothing().when(successStreamConsumer).accept(task, queryResult);
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenReturn(queryResult);
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    executor.executeTask(task).subscribe();

    verify(successStreamConsumer, times(1)).accept(Mockito.any(), Mockito.any());
    verify(failureStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
  }

  @Test
  public void whenTaskFailedWithAnyException() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();
    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(false);
    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());
    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return null;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };
    Mockito.doNothing().when(successStreamConsumer).accept(task, queryResult);
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenThrow(new RuntimeException());
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    executor.executeTask(task).subscribe();

    verify(successStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
    verify(failureStreamConsumer, times(1)).accept(Mockito.any(), Mockito.any());
  }

  @Test
  public void whenTaskFailedWithAnyExceptionForNativeQueryExecution() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD_FOR_NATIVE_QUERY)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();
    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(false);
    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());
    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return null;
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {

      }
    };
    Mockito.doNothing().when(successStreamConsumer).accept(task, queryResult);
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenThrow(new RuntimeException());
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    executor.executeTask(task).subscribe();

    verify(successStreamConsumer, times(0)).accept(Mockito.any(), Mockito.any());
    verify(failureStreamConsumer, times(1)).accept(Mockito.any(), Mockito.any());
  }

  @Test
  @SneakyThrows
  public void checkMonoIsLazilyExecuted() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return Iterators.empty();
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {
      }
    };

    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(false);

    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());

    Mockito.doNothing().when(successStreamConsumer).accept(task, queryResult);
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenThrow(new RuntimeException());
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    ThreadPoolExecutor executorService = new ThreadPoolExecutor(10, 10,
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    //Returns non but doesn't subscribe to it.
    Mono<QueryResult> queryResultMono = executor.executeTask(task);
    //Sleeps 5 seconds
    Thread.sleep(5000);
    //Verify basicQueryExecutor.execute is not called. Since mono is not subscribed.
    verify(basicQueryExecutor, times(0))
        .execute(Mockito.anyString(), Mockito.anyObject(), Mockito.any());

    //Subscribe to Mono.
    queryResultMono.subscribe();
    //Verify basicQueryExecutor.execute is called after mono is subscribed.
    verify(basicQueryExecutor, times(1))
        .execute(Mockito.anyString(), Mockito.anyObject(), Mockito.any());
  }

  @Test
  @SneakyThrows
  public void checkMonoIsLazilyExecutedForNativeQueryExecution() {
    BackgroundRefreshTask task = BackgroundRefreshTask.builder()
        .lockDao(lockDao)
        .queryPayload(SAMPLE_QUERY_PAYLOAD_FOR_NATIVE_QUERY)
        .remainingRetry(0)
        .executionAfterTimestamp(new Date().getTime())
        .backgroundRefresherConfig(BACKGROUND_REFRESHER_CONFIG).build();

    QueryResult queryResult = new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return Iterators.empty();
      }

      @Override
      public List<String> getColumns() {
        return columnNames;
      }

      @Override
      public void close() {
      }
    };

    Mockito.when(lockDao.isLock(Mockito.anyString())).thenReturn(false);

    Mockito.doNothing().when(lockDao).acquireLock(Mockito.anyString(), Mockito.anyLong());

    Mockito.doNothing().when(successStreamConsumer).accept(task, queryResult);
    Mockito.doNothing().when(failureStreamConsumer).accept(Mockito.any(), Mockito.any());
    Mockito.when(basicQueryExecutor.execute(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenThrow(new RuntimeException());
    Mockito.doNothing().when(executionAuditor).audit(Mockito.any());

    ThreadPoolExecutor executorService = new ThreadPoolExecutor(10, 10,
        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    BackgroundRefreshTaskExecutor executor = new BackgroundRefreshTaskExecutor(basicQueryExecutor,
        successStreamConsumer, failureStreamConsumer, executorService,
        executionAuditor, metricRegistry);
    //Returns non but doesn't subscribe to it.
    Mono<QueryResult> queryResultMono = executor.executeTask(task);
    //Sleeps 5 seconds
    Thread.sleep(5000);
    //Verify basicQueryExecutor.execute is not called. Since mono is not subscribed.
    verify(basicQueryExecutor, times(0))
        .execute(Mockito.anyString(), Mockito.anyObject(), Mockito.any());

    //Subscribe to Mono.
    queryResultMono.subscribe();
    //Verify basicQueryExecutor.execute is called after mono is subscribed.
    verify(basicQueryExecutor, times(1))
        .execute(Mockito.anyString(), Mockito.anyObject(), Mockito.any());
  }
}
