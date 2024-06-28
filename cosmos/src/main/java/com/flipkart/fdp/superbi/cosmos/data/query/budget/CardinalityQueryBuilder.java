package com.flipkart.fdp.superbi.cosmos.data.query.budget;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.DateRange;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.SelectColumnExp;
import com.flipkart.fdp.superbi.dsl.query.factory.DSQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.google.common.collect.Lists;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Has a strong association with
 * {@link com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy.HeuristicBudgetEstimationStrategy}
 */
public class CardinalityQueryBuilder extends AbstractQueryBuilder {

  private String from;
  private List<SelectColumnExp> columns = Lists.newArrayList();
  private List<DateRangePredicate> dateRangePredicates = Lists.newArrayList();
  private Optional<Criteria> criteriaOptional = Optional.empty();

  public CardinalityQueryBuilder(DSQuery query, Map<String, String[]> paramValues,
      AbstractDSLConfig config) {
    super(query, paramValues, config);
  }

  @Override
  public void visit(DateRangePredicate dateRangePredicate) {
    dateRangePredicates.add(dateRangePredicate);
  }

  @Override
  public void visitDateRange(String columnName, Date start, Date end) {
    dateRangePredicates.add(
        new DateRangePredicate(DateRange.builder().column(columnName).start(start).end(end).build())
    );
  }

  @Override
  public void visitHistogram(String alias, String columnName, long start, long end, long interval) {
    dateRangePredicates.add(
        new DateRangePredicate(DateRange.builder().column(columnName).start(start).end(end).build())
    );
  }

  @Override
  public void visitDateHistogram(String alias, String columnName, Date start, Date end,
                                 long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {
    dateRangePredicates.add(
        new DateRangePredicate(DateRange.builder().column(columnName).start(start).end(end).build())
    );
  }

  @Override
  public void visit(Criteria criteria) {
    criteriaOptional = Optional.ofNullable(criteria);
  }

  @Override
  public void visitFrom(String fromTable) {
    from = fromTable;
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    columns.add(ExprFactory.AGGR(groupByColumn, AggregationType.DISTINCT_COUNT));
  }

  public DSQuery build() {
    return (DSQuery) buildQueryImpl();
  }

  @Override
  protected Object buildQueryImpl() {
    return DSQueryBuilder.select(columns.toArray(new SelectColumnExp[columns.size()]))
        .from(from).where(criteriaOptional.orElse(null))
        .within(dateRangePredicates.toArray(new DateRangePredicate[dateRangePredicates.size()]))
        .build();
  }
}
