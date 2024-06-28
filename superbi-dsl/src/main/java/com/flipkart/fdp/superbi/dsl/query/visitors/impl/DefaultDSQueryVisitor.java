package com.flipkart.fdp.superbi.dsl.query.visitors.impl;

import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.DSQueryVisitor;
import com.google.common.base.Optional;
import java.util.Date;
import java.util.Map;

/**
 * User: shashwat
 * Date: 26/01/14
 */
public class DefaultDSQueryVisitor implements DSQueryVisitor {

    public final Map<String, String[]> paramValues;

    public DefaultDSQueryVisitor(Map<String, String[]> paramValues) {
        this.paramValues = paramValues;
    }

    @Override
    public void visit(SelectColumn.SimpleColumn column) {

    }

    @Override
    public void visit(SelectColumn.ConditionalAggregation column) {

    }

    @Override
    public void visit(SelectColumn.Aggregation column,
            SelectColumn.AggregationOptions options) {

    }


    @Override
    public void visit(DateRangePredicate dateRangePredicate) {
    }

    @Override
    public void visit(Criteria criteria) {
    }

    @Override
    public void visitFrom(String fromTable) {

    }

    @Override
    public void visitGroupBy(String groupByColumn) {

    }

    @Override
    public void visitDateHistogram(String alias, String columnName, Date from, Date to, long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {

    }

    @Override
    public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {

    }

    @Override
    public void visit(Optional<Integer> limit) {
    }

    @Override
    public void visitDateRange(String column, Date start, Date end) {

    }

    @Override
    public Map<String, String[]> getParamValues() {
        return paramValues;
    }

    @Override
    public void visitHistogram(String alias, String columnName, long start, long end, long interval) {
    }

    @Override
    public void visitSample(Integer integer) {

    }

}
