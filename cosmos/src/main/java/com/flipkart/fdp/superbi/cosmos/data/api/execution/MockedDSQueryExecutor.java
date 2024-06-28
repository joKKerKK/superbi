package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.google.common.collect.Lists;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by akshaya.sharma on 28/05/19
 */
@Deprecated
public class MockedDSQueryExecutor extends DSQueryExecutor{

  protected int timeToExecuteMs;
  protected int timeToExplainMs;

  protected Function<Void, QueryResult> returnQueryResultFunction;
  protected Function<Void, StreamingQueryResult> returnStreamingQueryResultFunction;

  public MockedDSQueryExecutor(String sourceName, int timeToExecuteMs, int timeToExplainMs) {
    this("MOCKED - " + sourceName, timeToExecuteMs, timeToExplainMs, null, null);
  }

  public MockedDSQueryExecutor(String sourceName, int timeToExecuteMs, int timeToExplainMs,
      Function<Void, QueryResult> returnQueryResultFunction, Function<Void, StreamingQueryResult> returnStreamingQueryResultFunction) {
    super("MOCKED - " + sourceName, null, null);
    if(timeToExecuteMs < 0) {
      timeToExecuteMs = 0;
    }
    if(timeToExplainMs < 0) {
      timeToExplainMs = 0;
    }
    this.timeToExecuteMs = timeToExecuteMs;
    this.timeToExplainMs = timeToExplainMs;
    this.returnQueryResultFunction = returnQueryResultFunction;
    this.returnStreamingQueryResultFunction = returnStreamingQueryResultFunction;
  }

  @Override
  public AbstractQueryBuilder getTranslator(DSQuery query, Map<String, String[]> paramValues) {
    return null;
  }

  @Override
  public Object explainNative(Object nativeQuery) {
    return "";
  }

  @Override
  public QueryResult executeNative(Object object, ExecutionContext context) {
    try {
      return waitAndReturnQueryResult();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private QueryResult waitAndReturnQueryResult() throws InterruptedException {
    Thread.sleep(timeToExecuteMs);
    if(returnQueryResultFunction != null) {
      return returnQueryResultFunction.apply(null);
    }
    return new QueryResult(null, Lists.newArrayList());
  }

  private StreamingQueryResult waitAndReturnStreamingQueryResult() throws InterruptedException {
    Thread.sleep(timeToExecuteMs);
    if(returnStreamingQueryResultFunction != null) {
      return returnStreamingQueryResultFunction.apply(null);
    }
    return null;
  }

  @Override
  public QueryResult executeNative(Object object, ExecutionContext context,
      ICacheClient<String, QueryResult> cacheClient) {
    try {
      return waitAndReturnQueryResult();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public StreamingQueryResult executeStreamNative(Object object,
      ExecutionContext context) {
    try {
      return waitAndReturnStreamingQueryResult();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
