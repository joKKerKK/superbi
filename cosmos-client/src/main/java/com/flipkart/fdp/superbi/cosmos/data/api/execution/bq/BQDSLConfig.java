package com.flipkart.fdp.superbi.cosmos.data.api.execution.bq;


import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.BQUsageType;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp.Type;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BQDSLConfig extends AbstractDSLConfig {

  @Getter
  private final Boolean isBadgeEnabled;

  public static final String IS_BADGER_ENABLED = "IS_BADGER_ENABLED";

  private static final Map<AggregationType, String> supportedAggregations = new HashMap<AggregationType, String>(){{
    put(AggregationType.COUNT, "count");
    put(AggregationType.DISTINCT_COUNT, "count");
    put(AggregationType.SUM, "sum");
    put(AggregationType.MAX, "max");
    put(AggregationType.MIN, "min");
    put(AggregationType.AVG, "avg");
    put(AggregationType.FRACTILE, "fractile");
    put(AggregationType.APPROX_UNIQUE_COUNT, "HLL_COUNT.MERGE");

  }};

  private static final Map<Type, String> supportedLogicalOps = new HashMap<Type, String>() {{
    put(Type.AND, "and");
    put(Type.OR, "or");
    put(Type.NOT, "!");
  }};

  private static final Map<Predicate.Type, String> supportedPredicates  = new HashMap<Predicate.Type, String>() {{
    put(Predicate.Type.eq, "=");
    put(Predicate.Type.neq, "!=");
    put(Predicate.Type.lt, "<");
    put(Predicate.Type.lte, "<=");
    put(Predicate.Type.gt, ">");
    put(Predicate.Type.gte, ">=");
    put(Predicate.Type.in, "in");
    put(Predicate.Type.not_in, "not in");
    put(Predicate.Type.like, "like");
    put(Predicate.Type.is_null, "is null");
    put(Predicate.Type.is_not_null, "is not null");
    put(Predicate.Type.native_filter, "");
  }};

  private static final Map<OrderByExp.Type, String> supportedOrderByTypes = new HashMap<OrderByExp.Type, String>() {{
    put(OrderByExp.Type.ASC, "asc");
    put(OrderByExp.Type.DESC, "desc");
  }};

  private static final String DEFAULT_TIME_ZONE = "Asia/Kolkata";
  private static final String DATE_KEY_SUFFIX = "date_key";
  private static final String TIME_KEY_SUFFIX = "time_key";

  @Getter
  private final BQUsageType usageType;

  @Getter
  private final boolean views;

  public static final String BQ_USAGE_TYPE = "BQ_USAGE_TYPE";
  public static final String BQ_REALTIME_VIEWS = "BQ_REALTIME_VIEWS";

  public BQDSLConfig(Map<String, String> overrides) {
    super(overrides);
    this.isBadgeEnabled = false;
    this.usageType = BQUsageType.valueOf(overrides.getOrDefault(BQ_USAGE_TYPE,BQUsageType.BQ_REALTIME.name()));
    this.views = Boolean.parseBoolean(overrides.getOrDefault(BQ_REALTIME_VIEWS, "false"));
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
    if (supportedOrderByTypes.containsKey(type)) {
      return supportedOrderByTypes.get(type);
    }
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getWrappedObject(Object object) {
    if (object instanceof String) {
      return String.format("\"%s\"", object);
    }
    return String.valueOf(object);
  }

  public static String getAggregationString(AggregationType type) {
    if (supportedAggregations.containsKey(type)) {
      return supportedAggregations.get(type);
    }
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getDefaultTimeZone() {
    return DEFAULT_TIME_ZONE;
  }

  public static String getLogicalOpString(Type type) {
    if (supportedLogicalOps.containsKey(type)) {
      return supportedLogicalOps.get(type);
    }
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getPredicateStringFor(Predicate.Type type) {
    if (supportedPredicates.containsKey(type)) {
      return supportedPredicates.get(type);
    }
    throw new RuntimeException("No handlers found for " + type);
  }

  public static String getColumnName(String fullyQualifiedName) {
    // We get the fully qualified name of the column - (index.type.colName) and returns the last part
    String[] colParts = fullyQualifiedName.split("\\.");
    return colParts[colParts.length - 1];
  }

  public String getDateKeySuffix() {
    return DATE_KEY_SUFFIX;
  }

  public String getTimeKeySuffix() {
    return TIME_KEY_SUFFIX;
  }

  public Optional<String> getMatchingTimeColumnIfPresent(String dateColumn, final String tableName) {
    if (StringUtils.isEmpty(dateColumn)) {
      return Optional.empty();
    }

    String[] dateColumnParts = dateColumn.split("\\.");
    if (dateColumnParts.length > 1) { //remove factname from dateColumn if it contains so.
      dateColumn = dateColumnParts[1];
    }
    String timeColumnName = dateColumn.replace(getDateKeySuffix(), getTimeKeySuffix());
    // We cant possibly predict time col if date col does not end with _date_key
    if (timeColumnName.equals(dateColumn)) {
      return Optional.empty();
    }
    Table table = MetaAccessor.get().getTableByName(tableName);
    for (Table.Column col : table.getColumns()) {
      if (timeColumnName.equals(col.getName())) {
        return Optional.of(timeColumnName);
      }
    }
    return Optional.empty();
  }

  public String getDateTimeSurrugatePattern() {
    return "yyyyMMddHHmm";
  }

  public String getDateSurrugatePattern() {
    return "yyyyMMdd";
  }

  public String getDateExpression(String dateColumn, String timeColumn) {
    return dateColumn + "*10000 + " + timeColumn;
  }

}
