package com.flipkart.fdp.superbi.dsl.query.visitors;

import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.google.common.base.Optional;
import java.util.Date;
import java.util.Map;

/**
 * User: shashwat
 * Date: 17/01/14
 */
public interface DSQueryVisitor {

    void visit(SelectColumn.SimpleColumn column);

    void visit(SelectColumn.ConditionalAggregation column);

    void visit(SelectColumn.Aggregation column,
        SelectColumn.AggregationOptions options);


    void visit(DateRangePredicate dateRangePredicate);

    void visit(Criteria criteria);

    void visitFrom(String fromTable);

    void visitGroupBy(String groupByColumn);

    void visitDateHistogram(String alias, String columnName, Date start, Date end, long intervalMs, SelectColumn.DownSampleUnit downSampleUnit);

    void visitOrderBy(String groupByColumn, OrderByExp.Type type);

    void visit(Optional<Integer> limit);

    void visitDateRange(String column, Date start, Date end);

    Map<String, String[]> getParamValues();

    void visitHistogram(String alias, String columnName, long start, long end, long interval);

    void visitSample(Integer integer);
}
