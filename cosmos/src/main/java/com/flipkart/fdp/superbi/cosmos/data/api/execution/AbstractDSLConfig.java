package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy.BudgetEstimationStrategy;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy.DefaultBudgetEstimationStrategy;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy.EstimationStrategySelector;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by amruth.s on 25-10-2014.
 */
public abstract class AbstractDSLConfig {

  /**
   * Make sure extending classes have immutable instances
   **/

  public static class Attributes {

    public static final String COND_AGGR_SUPPORTED = "conditionalAggrSupported";
    public static final String SLOW_QUERY_MS = "slowQueryTimeOutMs";
    public static final String QUERY_TIME_OUT_MS = "queryTimeOutMs";
    public static final String MAX_ACTIVE_CONNECTIONS = "max_active_connections";
    public static final String DEFAULT_DATABASE = "defaultDatabase";
    public static final String IS_CACHE_ENABLED = "isCacheEnabled";
    public static final String DEFAULT_TTL_IN_SEC = "defaultTTLInSec";
    public static final String BUDGET_CACHE_KEY_TTL_IN_SEC = "budgetCacheKeyTTLInSec";
    public static final String QUERY_BUDGET_ESTIMATION = "queryBudgetEstimation";
    public static final String CACHE_POLLING_TIME_MS = "cachePollingTimeMs";
    public static final String BUDGET_THRESHOLD = "budgetThreshold";
    public static final String USE_PLACEHOLDER = "usePlaceholder";
    public static final String POST_PROCESSING_TTL_IN_SEC = "postProcessingTTLInSec";
    public static final String QUERY_POLLING_FREQUENCY_IN_SEC = "queryPollingFrequencyInSec";
    public static final String FAILED_QUERY_TTL_IN_SEC = "failedQueryTTLInSec";


    //TODO please put this in an util
    public static Map<String, String> getOverrides(Map<String, String> baseMap,
        Map<String, String> overrides) {
      Map<String, String> attributes = Maps.newHashMap(baseMap);
      attributes.putAll(overrides);
      return attributes;
    }

  }

  private boolean conditionalAggrSupported = false;

  private long slowQueryTimeOutMs = 5000;
  private boolean usePlaceHolder = true;
  private long queryTimeOutMs = 180000;

  private int maxActiveConnections = 10;
  private int defaultTTLInSec = 1 * 24 * 3600;
  private int budgetCacheKeyTTLInSec = 3600;
  private int postProcessingTTLInSec = 10;
  private int queryPollingFrequencyInSec = 30;
  private int cachePollingTimeMs = 1000;
  private int failedQueryTTLInSec = 30 * 60; // 30 mins
  private boolean isCacheEnabled = false;
  private long budgetThreshold = 150000;
  protected BudgetEstimationStrategy budgetEstimationStrategy = new DefaultBudgetEstimationStrategy();

  public AbstractDSLConfig(Map<String, String> overrides) {
    /* TODO Refactor this when moved to java 8 */
    if (overrides.containsKey(Attributes.COND_AGGR_SUPPORTED)) {
      conditionalAggrSupported = Boolean.valueOf(overrides.get(Attributes.COND_AGGR_SUPPORTED));
    }
    if (overrides.containsKey(Attributes.SLOW_QUERY_MS)) {
      slowQueryTimeOutMs = Long.valueOf(overrides.get(Attributes.SLOW_QUERY_MS));
    }
    if (overrides.containsKey(Attributes.QUERY_TIME_OUT_MS)) {
      queryTimeOutMs = Long.valueOf(overrides.get(Attributes.QUERY_TIME_OUT_MS));
    }
    if (overrides.containsKey(Attributes.MAX_ACTIVE_CONNECTIONS)) {
      maxActiveConnections = Integer.valueOf(overrides.get(Attributes.MAX_ACTIVE_CONNECTIONS));
    }
    if (overrides.containsKey(Attributes.IS_CACHE_ENABLED)) {
      isCacheEnabled = Boolean.valueOf(overrides.get(Attributes.IS_CACHE_ENABLED));
    }
    if (overrides.containsKey(Attributes.DEFAULT_TTL_IN_SEC)) {
      defaultTTLInSec = Integer.valueOf(overrides.get(Attributes.DEFAULT_TTL_IN_SEC));
    }
    if (overrides.containsKey(Attributes.QUERY_BUDGET_ESTIMATION)) {
      budgetEstimationStrategy = EstimationStrategySelector.instance.select(overrides);
    }
    if (overrides.containsKey(Attributes.CACHE_POLLING_TIME_MS)) {
      cachePollingTimeMs = Integer.valueOf(overrides.get(Attributes.CACHE_POLLING_TIME_MS));
    }
    if (overrides.containsKey(Attributes.FAILED_QUERY_TTL_IN_SEC)) {
      failedQueryTTLInSec = Integer.valueOf(overrides.get(Attributes.FAILED_QUERY_TTL_IN_SEC));
    }
    if (overrides.containsKey(Attributes.BUDGET_CACHE_KEY_TTL_IN_SEC)) {
      budgetCacheKeyTTLInSec = Integer.valueOf(
          overrides.get(Attributes.BUDGET_CACHE_KEY_TTL_IN_SEC));
    }
    if (overrides.containsKey(Attributes.BUDGET_THRESHOLD)) {
      budgetThreshold = Integer.valueOf(overrides.get(Attributes.BUDGET_THRESHOLD));
    }
    if (overrides.containsKey(Attributes.USE_PLACEHOLDER)) {
      usePlaceHolder = Boolean.valueOf(overrides.get(Attributes.USE_PLACEHOLDER));
    }
    if (overrides.containsKey(Attributes.POST_PROCESSING_TTL_IN_SEC)) {
      postProcessingTTLInSec = Integer.valueOf(
          overrides.get(Attributes.POST_PROCESSING_TTL_IN_SEC));
    }
    if (overrides.containsKey(Attributes.QUERY_POLLING_FREQUENCY_IN_SEC)) {
      queryPollingFrequencyInSec = Integer.valueOf(
          overrides.get(Attributes.QUERY_POLLING_FREQUENCY_IN_SEC));
    }
    if (overrides.containsKey(Attributes.FAILED_QUERY_TTL_IN_SEC)) {
      failedQueryTTLInSec = Integer.valueOf(overrides.get(Attributes.FAILED_QUERY_TTL_IN_SEC));
    }
  }

  public final boolean isConditionalAggrSupported() {
    return conditionalAggrSupported;
  }

  public final boolean isCacheEnabled() {
    return isCacheEnabled;
  }

  public final long getSlowQueryTimeOutMs() {
    return slowQueryTimeOutMs;
  }

  public final long getQueryTimeOutMs() {
    return queryTimeOutMs;
  }

  public final int getMaxActiveConnections() {
    return maxActiveConnections;
  }

  public int getDefaultTTLInSec() {
    return defaultTTLInSec;
  }

  public boolean usePlaceHolder() {
    return usePlaceHolder;
  }

  public int getBudgetCacheKeyTTLInSec() {
    return budgetCacheKeyTTLInSec;
  }

  public long getBudgetThreshold() {
    return budgetThreshold;
  }

  public int getCachePollingTimeMs() {
    return cachePollingTimeMs;
  }

  public int getPostProcessingTTLInSec() {
    return postProcessingTTLInSec;
  }

  public int getQueryPollingFrequencyInSec() {
    return queryPollingFrequencyInSec;
  }

  public int getFailedQueryTTLInSec() {
    return failedQueryTTLInSec;
  }

  public final BudgetEstimationStrategy getEstimationStrategy() {
    return budgetEstimationStrategy;
  }

  public abstract Set<LogicalOp.Type> getSupportedLogicalOperators();

  public abstract Set<Predicate.Type> getSupportedPredicates();

  public abstract Set<AggregationType> getSupportedAggregations();

  public List<? extends Object> getInitScripts() {
    return Lists.newArrayList();
  }
}
