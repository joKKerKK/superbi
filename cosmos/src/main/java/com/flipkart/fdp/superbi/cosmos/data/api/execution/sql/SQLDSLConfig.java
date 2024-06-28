package com.flipkart.fdp.superbi.cosmos.data.api.execution.sql;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig.Attributes.*;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig.Attributes
    .COND_AGGR_SUPPORTED;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig.Attributes
    .DEFAULT_DATABASE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig.Attributes
    .QUERY_TIME_OUT_MS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig.Attributes
    .getOverrides;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.Predicate.Type;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;

/**
 * Created by rajesh.kannan on 28/05/15.
 */

public abstract class SQLDSLConfig extends AbstractDSLConfig {

    private String date_surrugate_key_suffix = "date_key";
    private String time_surrugate_key_suffix = "time_key";

    private static Map<String, String> staticAttributes = Maps.newHashMap();
    static {
        staticAttributes.put(DEFAULT_DATABASE, "bigfoot_external_neo");
        staticAttributes.put(COND_AGGR_SUPPORTED, "true");
        staticAttributes.put(QUERY_TIME_OUT_MS, "300000");
    }

    private static Map<AggregationType, String> supportedAggregations = Maps.newHashMap();
    private static Map<LogicalOp.Type, String> supportedLogicalOps = Maps.newHashMap();
    private static Map<Predicate.Type, String> supportedPredicates = Maps.newHashMap();
    private static Map<OrderByExp.Type, String> supportedOrderByTypes = Maps.newHashMap();
    static {
        supportedAggregations.put(AggregationType.COUNT, "count");
        supportedAggregations.put(AggregationType.DISTINCT_COUNT, "count");
        supportedAggregations.put(AggregationType.SUM, "sum");
        supportedAggregations.put(AggregationType.MAX, "max");
        supportedAggregations.put(AggregationType.MIN, "min");
        supportedAggregations.put(AggregationType.AVG, "avg");
        supportedAggregations.put(AggregationType.FRACTILE, "fractile");

        supportedLogicalOps.put(LogicalOp.Type.AND, "and");
        supportedLogicalOps.put(LogicalOp.Type.OR, "or");
        supportedLogicalOps.put(LogicalOp.Type.NOT, "!");

        supportedPredicates.put(Predicate.Type.eq, "=");
        supportedPredicates.put(Predicate.Type.neq, "!=");
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

    public SQLDSLConfig(Map<String, String> overrides) {
        super(getOverrides(staticAttributes, overrides));
    }

    @Override
    public Set<LogicalOp.Type> getSupportedLogicalOperators() {
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

    public String getDateKeySuffix() {
        return date_surrugate_key_suffix;
    }

    public String getTimeKeySuffix() {
        return time_surrugate_key_suffix;
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
            return "'" + String.valueOf(object) + "'";
        } else {
            return String.valueOf(object);
        }
    }

    public Optional<String> getMatchingTimeColumnIfPresent(String dateColumn, final String cosmosTableName) {
        String[] dateColumnParts = dateColumn.split("\\.");
        if(dateColumnParts.length > 1){ //remove factname from dateColumn if it contains so.
            dateColumn = dateColumnParts[1];
        }

        String timeColumnName = dateColumn.replace(getDateKeySuffix(), getTimeKeySuffix());

        // We cant possibly predict time col if date col does not end with _date_key
        if(timeColumnName.equals(dateColumn)) return Optional.absent();
        Table table = MetaAccessor.get().getTableByName(cosmosTableName);
        for(Table.Column col: table.getColumns()) {
            if(col.getName().equals(timeColumnName)) {
                return Optional.of(timeColumnName);
            }
        }
        return Optional.absent();
    }

    protected boolean isDateColumnDegenerateDimension(String dateColumn, String factName) {
        String[] dateColumnParts = dateColumn.split("\\.");
        if(dateColumnParts.length > 1){ //remove factname from dateColumn if it contains so.
            dateColumn = dateColumnParts[1];
        }
        Fact fact = MetaAccessor.get().getFactByName(factName);
        for(Fact.DimensionMapping dimensionMapping : fact.getDimensionsMapping()){
            if(dimensionMapping.getFactColumnName().equals(dateColumn)){
                return false;
            }
        }
        return true;
    }


    public String getDateTimeSurrugatePattern() {
        return "yyyyMMddHHmm";
    }

    public String getTimestampPattern() {
        return "yyyy-MM-dd HH:mm:ss";
    }

    public String getDateSurrugatePattern() {
        return "yyyyMMdd";
    }

    public String getDateExpression(String dateColumn, String timeColumn) {
        return dateColumn + "*10000 + " + timeColumn;
    }

    public static String getOrderByType(OrderByExp.Type type) {
        String orderByClauseType = supportedOrderByTypes.get(type);
        if(orderByClauseType == null) {
            throw new RuntimeException("Order by clause map not found for " + type);
        }
        return orderByClauseType;
    }

    public String getDatabase()
    {
        return staticAttributes.get(DEFAULT_DATABASE);

    }

}
