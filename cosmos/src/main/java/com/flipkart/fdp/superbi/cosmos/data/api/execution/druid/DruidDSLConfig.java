package com.flipkart.fdp.superbi.cosmos.data.api.execution.druid;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig.Attributes.getOverrides;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp.Type;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

public class DruidDSLConfig extends AbstractDSLConfig{

  public static final String DEFAULT_TIME_ZONE = "Asia/Kolkata";
  public static final long DEFAULT_QUERY_TIMEOUT = 120000;
  public static final String SQL_TIME_ZONE_KEY = "sqlTimeZone";
  public static final String QUERY_TIMEOUT = "druid.server.http.defaultQueryTimeout";
  public static final String DISTINCT_COUNT_KEY = "APPROX_COUNT_DISTINCT_DS_THETA";
  public static final String TIME_ZONE_ATTRIBUTE = "TIME_ZONE";
  public static final String QUERY_TIMEOUT_ATTRIBUTE = "QUERY_TIMEOUT";
  public static final String DISTINCT_COUNT_ATTRIBUTE = "DISTINCT_COUNT";
  public static final String ENABLE_VALIDATIONS = "enableValidations";

  private static Map<String, String> staticAttributes = Maps.newHashMap();


  private static Map<OrderByExp.Type, String> supportedOrderByTypes = Maps.newHashMap();

  private static Map<AggregationType, String> supportedAggregations = Maps.newHashMap();
  private static Map<LogicalOp.Type, String> supportedLogicalOps = Maps.newHashMap();
  private static Map<Predicate.Type, String> supportedPredicates = Maps.newHashMap();

  @Getter
  private final Boolean enableValidations;

  static {

    supportedAggregations.put(AggregationType.COUNT, "count");
    supportedAggregations.put(AggregationType.DISTINCT_COUNT, getDistinctAggregation());
    supportedAggregations.put(AggregationType.SUM, "sum");
    supportedAggregations.put(AggregationType.MAX, "max");
    supportedAggregations.put(AggregationType.MIN, "min");
    supportedAggregations.put(AggregationType.AVG, "avg");

    supportedLogicalOps.put(LogicalOp.Type.AND, "and");
    supportedLogicalOps.put(LogicalOp.Type.OR, "or");
    supportedLogicalOps.put(LogicalOp.Type.NOT, "!");

    supportedPredicates.put(Predicate.Type.eq, "=");
    supportedPredicates.put(Predicate.Type.neq, "<>");
    supportedPredicates.put(Predicate.Type.lt, "<");
    supportedPredicates.put(Predicate.Type.lte, "<=");
    supportedPredicates.put(Predicate.Type.gt, ">");
    supportedPredicates.put(Predicate.Type.gte, ">=");
    supportedPredicates.put(Predicate.Type.in, "in");
    supportedPredicates.put(Predicate.Type.not_in, "not in");
    supportedPredicates.put(Predicate.Type.like, "like");
    supportedPredicates.put(Predicate.Type.is_null, "is null");
    supportedPredicates.put(Predicate.Type.is_not_null, "is not null");
    supportedPredicates.put(Predicate.Type.native_filter, "");



    supportedOrderByTypes.put(OrderByExp.Type.ASC, "asc");
    supportedOrderByTypes.put(OrderByExp.Type.DESC, "desc");
  }

  public DruidDSLConfig(Map<String, String> overrides) {
    super(getOverrides(staticAttributes, overrides));
    this.enableValidations = Boolean.valueOf(overrides.getOrDefault(ENABLE_VALIDATIONS, "false"));
    staticAttributes.put(TIME_ZONE_ATTRIBUTE,overrides.get(TIME_ZONE_ATTRIBUTE));
    staticAttributes.put(DISTINCT_COUNT_ATTRIBUTE,overrides.get(DISTINCT_COUNT_ATTRIBUTE));
    staticAttributes.put(QUERY_TIMEOUT_ATTRIBUTE,overrides.get(QUERY_TIMEOUT_ATTRIBUTE));
  }

  public static String getTimeZone(){
    return staticAttributes.get(TIME_ZONE_ATTRIBUTE) != null ? staticAttributes.get(
        TIME_ZONE_ATTRIBUTE) : DEFAULT_TIME_ZONE;
  }

  public static long getQueryTimeout(){
    return staticAttributes.get(QUERY_TIMEOUT_ATTRIBUTE) != null ? Long.valueOf(staticAttributes.get("QUERY_TIMEOUT")) : DEFAULT_QUERY_TIMEOUT;
  }

  public static String getDistinctAggregation(){
    return staticAttributes.get(DISTINCT_COUNT_ATTRIBUTE) != null ? staticAttributes.get("DISTINCT_COUNT") : DISTINCT_COUNT_KEY;
  }

  @Override
  public Set<Type> getSupportedLogicalOperators() {
    return supportedLogicalOps.keySet();
  }

  @Override
  public Set<Predicate.Type> getSupportedPredicates() {
    return supportedPredicates.keySet();
  }

  @Override
  public Set<AggregationType> getSupportedAggregations() {
    return supportedAggregations.keySet();
  }

  public static String getOrderByType(OrderByExp.Type type) {
    String orderByClauseType = supportedOrderByTypes.get(type);
    if(orderByClauseType == null) {
      throw new RuntimeException("Order by clause map not found for " + type);
    }
    return orderByClauseType;
  }

  public static String getAggregationString(AggregationType type) {
    if(supportedAggregations.containsKey(type))
      return supportedAggregations.get(type);
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getLogicalOpString(LogicalOp.Type type) {
    if(supportedLogicalOps.containsKey(type))
      return supportedLogicalOps.get(type);
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getPredicateStringFor(Predicate.Type type) {
    if(supportedPredicates.containsKey(type))
      return supportedPredicates.get(type);
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getWrappedObject(Object object) {
    if(object instanceof String) {
      String val = String.valueOf(object);
      if (val.contains("'")) {
        val = val.replace("'","''");
      }
      return "'" + val + "'";
    } else {
      return String.valueOf(object);
    }
  }
}
