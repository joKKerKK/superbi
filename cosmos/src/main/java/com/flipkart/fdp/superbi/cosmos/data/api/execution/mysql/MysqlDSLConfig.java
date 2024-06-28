package com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.dsl.query.*;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;


public class MysqlDSLConfig extends AbstractDSLConfig {

    private static Map<AggregationType, String> supportedAggregations = Maps.newHashMap();

    static {
        supportedAggregations.put(AggregationType.COUNT, "count");
        supportedAggregations.put(AggregationType.DISTINCT_COUNT, "count");
        supportedAggregations.put(AggregationType.SUM, "sum");
        supportedAggregations.put(AggregationType.MAX, "max");
        supportedAggregations.put(AggregationType.MIN, "min");
        supportedAggregations.put(AggregationType.AVG, "avg");
    }

    public MysqlDSLConfig(Map<String, String> overrides) {
            super(overrides);
        }

    @Override
    public java.util.Set<LogicalOp.Type> getSupportedLogicalOperators() {
        return Sets.newHashSet(LogicalOp.Type.values());
    }

    @Override
    public java.util.Set<Predicate.Type> getSupportedPredicates() {
        return Sets.newHashSet(Predicate.Type.values());
    }

    @Override
    public java.util.Set<AggregationType> getSupportedAggregations() {
        return Sets.newHashSet(AggregationType.values());
    }

    private static Map<OrderByExp.Type, String> orderByClauseMap = Maps.newHashMap();
    static {
        orderByClauseMap.put(OrderByExp.Type.ASC, "asc");
        orderByClauseMap.put(OrderByExp.Type.DESC, "desc");
    }

    public static String getOrderByType(OrderByExp.Type type) {
        String orderByClauseType = orderByClauseMap.get(type);
        if(orderByClauseType == null) {
            throw new RuntimeException("Order by clause map not found for " + type);
        }
        return orderByClauseType;
    }

    public static String getWrappedObject(Object object) {
        if(object instanceof String) {
            return "'" + String.valueOf(object) + "'";
        } else {
            return String.valueOf(object);
        }
    }
}