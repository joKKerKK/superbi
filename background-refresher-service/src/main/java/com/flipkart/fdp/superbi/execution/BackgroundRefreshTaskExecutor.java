package com.flipkart.fdp.superbi.execution;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.flipkart.fdp.superbi.utils.JsonUtil;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class BackgroundRefreshTaskExecutor {

  private static final int MAX_STRING_SIZE = 15000;
  private static final String NATIVE_QUERY_SERDE_SUCCESS_METRIC_KEY = "nativeQuery.serde.success";
  private static final String NATIVE_QUERY_SERDE_FAILURE_METRIC_KEY = "nativeQuery.serde.failure";
  private final BasicQueryExecutor basicQueryExecutor;
  private final BiConsumer<BackgroundRefreshTask, QueryResult> successStreamConsumer;
  private final BiConsumer<BackgroundRefreshTask, Throwable> failureStreamConsumer;
  private final ExecutionAuditor executionAuditor;
  private final Scheduler scheduler;
  private final MetricRegistry metricRegistry;

  public BackgroundRefreshTaskExecutor(
      BasicQueryExecutor basicQueryExecutor,
      BiConsumer<BackgroundRefreshTask, QueryResult> successStreamConsumer,
      BiConsumer<BackgroundRefreshTask, Throwable> failureStreamConsumer,
      ExecutorService executorService,
      ExecutionAuditor executionAuditor, MetricRegistry metricRegistry) {
    this.basicQueryExecutor = basicQueryExecutor;
    this.successStreamConsumer = successStreamConsumer;
    this.failureStreamConsumer = failureStreamConsumer;
    this.executionAuditor = executionAuditor;
    scheduler = Schedulers.fromExecutorService(executorService);
    this.metricRegistry = metricRegistry;
  }

  private static String truncateString(String sourceString) {
    return sourceString == null ? sourceString
        : sourceString.substring(0, Math.min(sourceString.length(), MAX_STRING_SIZE));
  }

  @SneakyThrows
  public Mono<QueryResult> executeTaskAsync(BackgroundRefreshTask task) {
      log.info("BackgroundRefreshTaskExecutor - executeTaskAsync called");
    return executeTask(task).subscribeOn(scheduler);
  }

  //Task unlock is handled in success and failure consumers
  @SneakyThrows
  public Mono<QueryResult> executeTask(BackgroundRefreshTask task) {
    log.info("BackgroundRefreshTaskExecutor - executeTask called");
    String storeIdentifier = task.getQueryPayload().getStoreIdentifier();
    long startTime = System.currentTimeMillis();
    if (!task.isLock()) {
      log.info("acquiring lock for cacheKey {}", task.getCacheKey());
      task.acquireLock(
          (int) (task.getBackgroundRefresherConfig().getLockTimeoutInMillies() / 1000));
      int attemptNumber = task.getAttemptNumber();
      String requestId = task.getQueryPayload().getRequestId();

      ExecutionLog.ExecutionLogBuilder executionLogBuilder = ExecutionLog.builder();
      log.info("executing query for requestId -<{}> and attemptNumber - <{}> and cacheKey- <{}>",
          requestId, attemptNumber, task.getCacheKey());
      return executeTaskAsync(task,
          storeIdentifier, executionLogBuilder).doOnSuccess(
          queryResult -> {
            successStreamConsumer.accept(task, queryResult);
            executionLogBuilder.isCompleted(true);
          }
      ).doOnError(
          throwable -> {
            log.error(
                "Task execution failed for requestId - <{}> and attemptNumber- <{}> and cacheKey - <{}> with exception {}"
                , requestId, attemptNumber, task.getCacheKey(), throwable.getMessage());

              // Getting stack trace as string
              StringWriter sw = new StringWriter();
              PrintWriter pw = new PrintWriter(sw);
              throwable.printStackTrace(pw);
            executionLogBuilder.message(sw.toString());
            executionLogBuilder.isCompleted(false);

            failureStreamConsumer.accept(task, throwable);
          }
      ).doFinally(
          signalType -> {
            long elapsedTime = System.currentTimeMillis() - startTime;
            executionLogBuilder.totalTimeMs(elapsedTime).requestId(requestId)
                .attemptNumber(task.getAttemptNumber()).startTimeStampMs(startTime).id(
                    UUID.randomUUID().toString());
            if(task.getQueryPayload().getNativeQuery() != null){
              executionLogBuilder.translatedQuery(JsonUtil.toJson(task.getQueryPayload().getNativeQuery().getQuery()));
              executionLogBuilder.cacheHit(false);
              if (task.getQueryPayload().getMetaDataPayload().getFactName() != null) {
                executionLogBuilder.factName(task.getQueryPayload().getMetaDataPayload().getFactName());
              }
                log.info("BackgroundRefreshTaskExecutor - executeTask - executionLogBuilder.cacheHit(false)");
            }
              executionAuditor.audit(executionLogBuilder.build());
              log.info("BackgroundRefreshTaskExecutor - executeTask - executionLogBuilder.cacheHit(false) DONE");
          }
      );
    } else {
      return Mono.empty();
    }
  }

  private Mono<QueryResult> executeTaskAsync(BackgroundRefreshTask task, String storeIdentifier,
      ExecutionLog.ExecutionLogBuilder executionLogBuilder) {

    DataSourceQuery dataSourceQuery = DataSourceQuery.builder()
        .nativeQuery(task.getQueryPayload().getNativeQuery().getQuery())
        .cacheKey(task.getCacheKey())
        .metaDataPayload(task.getQueryPayload().getMetaDataPayload())
        .build();

    return Mono.fromCallable(
        () -> basicQueryExecutor.execute(storeIdentifier, dataSourceQuery,
            executionLogBuilder));
  }
}

