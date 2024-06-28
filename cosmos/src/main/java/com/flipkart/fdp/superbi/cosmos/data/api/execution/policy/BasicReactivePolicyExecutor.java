package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.functions.Action1;

@Slf4j
public class BasicReactivePolicyExecutor<T extends QueryResult> implements Executable<T> {

  protected long elapsedTime;
  private Action1<QueryResult> successCallback;
  private Action1<Throwable> errorCallback;
  private ObjectMapper mapper = new ObjectMapper();

  public BasicReactivePolicyExecutor(Action1<QueryResult> successCallback,
      Action1<Throwable> errorCallback) {
    if (null == successCallback) {
      successCallback = s -> {
      };
    }
    if (null == errorCallback) {
      errorCallback = e -> {
      };
    }
    this.successCallback = successCallback;
    this.errorCallback = errorCallback;
  }

  @Override
  public long elapsedTimeMs() {
    return elapsedTime;
  }

  @Override
  @LogExecTime
  @SneakyThrows
  public Optional<T> execute(ExecutionContext context, NativeQueryTranslator translator) {

    final Timer elapsedTimer = new Timer().start();

    String threadPool = context.getSourceName();
    String federation = context.getFederationType().toString();
    DSQueryExecutor executor = context.getExecutor();
    Object nativeQuery = translator.getTranslatedQuery();
    DSQuery dsQuery = context.getQuery();
    Map<String, String[]> params = context.getParams();

    int timeoutMs = (int) context.getConfig().getQueryTimeOutMs();

    ExecutorQueryInfoLog executorQueryInfoLog = null;
    try {
      executorQueryInfoLog = new ExecutorQueryInfoLog.Builder()
          .setDsQuery(mapper.writeValueAsString(dsQuery))
          .setSourceName(threadPool + "_" + federation)
          .setSourceType(context.getSourceType())
          .setTranslatedQuery(mapper.writeValueAsString(translator.getTranslatedQuery()))
          .setStartTimeStampMs(elapsedTimer.getStartTimeMs())
          .setTranslationTimeMs(translator.getElapsedTimeMs())
          .setCacheHit(false)
          .setAttemptNumber(context.getAttemptNumber())
          .setRequestId(context.getRequestId())
          .setFactName(dsQuery.getFromTable())
          .build();
    } catch (Exception exception) {

    }

    try {
      Observable<QueryResult> queryResultObservable = new RemoteCall.Builder<QueryResult>(
          threadPool, federation)
          .withTimeOut(timeoutMs).around(new ActualCall<QueryResult>() {
            public QueryResult workUnit() {
              return executor.executeNative(nativeQuery,
                  new DSQueryExecutor.ExecutionContext(dsQuery, params));
            }
          }).executeReactive();

      ExecutorQueryInfoLog finalExecutorQueryInfoLog = executorQueryInfoLog;
      Action1<com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult>
          internalSuccessCallback = (s) -> {
        try {
          log.info("Raw Query result from sink recieved for Request: <{}>", context.getRequestId());
          if (finalExecutorQueryInfoLog != null) {
            long executionTime =
                new Date().getTime() - finalExecutorQueryInfoLog.getStartTimeStampMs();
            finalExecutorQueryInfoLog.setExecutionTimeMs(executionTime);
            finalExecutorQueryInfoLog.setCompleted(true);
            finalExecutorQueryInfoLog
                .setSlowQuery(executionTime > context.getConfig().getSlowQueryTimeOutMs());
            MetaCreator.get().logExecutorQueryInfo(finalExecutorQueryInfoLog);
          }
        } finally {
          successCallback.call(s);
        }
      };

      Action1<Throwable> internalErrorCallback = (s) -> {
        try {
          log.info("Query execution is failed for Request: {}", context.getRequestId());
          if (finalExecutorQueryInfoLog != null) {
            long executionTime =
                new Date().getTime() - finalExecutorQueryInfoLog.getStartTimeStampMs();
            finalExecutorQueryInfoLog.setExecutionTimeMs(executionTime);
            finalExecutorQueryInfoLog.setCompleted(false);
            finalExecutorQueryInfoLog
                .setSlowQuery(executionTime > context.getConfig().getSlowQueryTimeOutMs());
            MetaCreator.get().logExecutorQueryInfo(finalExecutorQueryInfoLog);
          }
        } finally {
          errorCallback.call(s);
        }
      };

      queryResultObservable.subscribe(internalSuccessCallback, internalErrorCallback);
      return Optional.empty();
    } finally {
      elapsedTimer.stop();
      elapsedTime = elapsedTimer.getTimeTakenMs();
    }
  }
}