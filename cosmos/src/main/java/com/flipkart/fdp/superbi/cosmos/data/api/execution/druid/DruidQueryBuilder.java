package com.flipkart.fdp.superbi.cosmos.data.api.execution.druid;


import static com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig.getAggregationString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig.getLogicalOpString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig.getPredicateStringFor;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig.getWrappedObject;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.Predicate.Type;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.CompositeColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Getter
public class DruidQueryBuilder extends AbstractQueryBuilder{

  private final DruidDSLConfig castedConfig;
  private List<String> selectExpressions = Lists.newArrayList();
  private List<String> groupByExpressions = Lists.newArrayList();
  private List<String> orderByExpressions = Lists.newArrayList();
  private final List<String> rootCriteriaString = Lists.newArrayList();
  private Optional<Integer> limit = Optional.absent();
  private String from;
  private List<String> headers = new ArrayList<>();

  public DruidQueryBuilder(DSQuery query, Map<String, String[]> values, DruidDSLConfig config) {
    super(query, values, config);
    castedConfig = config;
  }

  @Override public void visit(SelectColumn.SimpleColumn column) {
    String selectExpression = new StringBuffer()
        .append(column.isNativeExpression ? "" : "\"")
        .append(column.isNativeExpression ? column.colName : getModifiedColumn(column.colName))
        .append(column.isNativeExpression ? "" : "\"")
        .append(" as ")
        .append(column.getAlias())
        .toString();
    selectExpressions.add(
        selectExpression);
    headers.add(column.getAlias());
  }

  @Override
  public void visit(SelectColumn.Aggregation column,
      SelectColumn.AggregationOptions options) {

      selectExpressions.add(
          new StringBuffer()
              .append(getAggregationString(
                  column.aggregationType))
              .append("(\"")
              .append(getModifiedColumn(column.colName))
              .append("\")")
              .append(" as \"")
              .append(column.getAlias())
              .append("\"")
              .toString()
      );
    headers.add(column.getAlias());
  }

  @Override
  public void visit(SelectColumn.ConditionalAggregation column) {
    final RootCriteriaBuilder caseCriteriaBuilder = new RootCriteriaBuilder();
    column.criteria.accept(caseCriteriaBuilder);
    final String caseExpression = " CASE WHEN "
        + caseCriteriaBuilder.criteriaBuilder.toString()
        + " THEN " + column.colName + " END";
    selectExpressions.add(
        new StringBuffer()
            .append(column.type)
            .append("(")
            .append(caseExpression)
            .append(")")
            .append(" as \"")
            .append(column.getAlias())
            .append("\"")
            .toString()
    );
    headers.add(column.getAlias());
  }

  @Override
  public void visit(DateRangePredicate dateRangePredicate) {
    final DruidQueryBuilder.RootCriteriaBuilder filterBuilder = new DruidQueryBuilder.RootCriteriaBuilder();
    dateRangePredicate.accept(filterBuilder);
    final String criteriaString = filterBuilder.criteriaBuilder.toString();
    if (!criteriaString.equals("")) {
      rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
    }
  }

  @Override
  public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
    String bucketExpression = "(floor(" + getModifiedColumn(columnName) +  "/" + interval + ")" + "*" + interval + ")";
    String histogramExpr = " " + bucketExpression +  " as " + alias + " ";
    selectExpressions.add(histogramExpr);
    rootCriteriaString.add(bucketExpression + ">=" + from);
    rootCriteriaString.add(bucketExpression + "<=" + to);
    groupByExpressions.add(bucketExpression);
    orderByExpressions.add(bucketExpression);
    headers.add(alias);
  }

  @Override
  public void visitDateRange(String column, Date start, Date end) {
    final String bucketExpression = "TIMESTAMP_TO_MILLIS(" + getModifiedColumn(column) + ")";
    final StringBuilder dateRangeBuilder = new StringBuilder(bucketExpression);
    dateRangeBuilder.append(">=")
        .append(start.getTime());
    if(end != null) {
      dateRangeBuilder .append(" and ")
          .append(bucketExpression)
          .append("<")
          .append(end.getTime());
    }
    rootCriteriaString.add(dateRangeBuilder.toString());
  }

  @Override public void visitDateHistogram(String alias, String columnName,
                                           Date from, Date to, long intervalMs,
                                           SelectColumn.DownSampleUnit downSampleUnit) {
    final Long GMT_TIMEZONE_OFFSET = 19800000L;

    final String bucketExpression = "TIMESTAMP_TO_MILLIS(" + getModifiedColumn(columnName) + ")";
    final String bucketExpressionTimezoneAdjusted = bucketExpression + " + " + GMT_TIMEZONE_OFFSET;

    final String bucketExpressionFinal = "(" + bucketExpressionTimezoneAdjusted + ")" + "/" + intervalMs;

      String bucketExpressionWithInterval = "MILLIS_TO_TIMESTAMP(" +
          "(" +
          "FLOOR(" + bucketExpressionFinal + ")) *" +intervalMs + "-" + GMT_TIMEZONE_OFFSET+ ")";

    String selectHistogramExpr = bucketExpressionWithInterval
        + " as \"" + alias + "\"";

    /*selectHistogramExpr will be like MILLIS_TO_TIMESTAMP((FLOOR((TIMESTAMP_TO_MILLIS(columnName) + 19800000)/5000)) *5000-19800000) as "alias"*/

    selectExpressions.add(selectHistogramExpr);
    rootCriteriaString.add(bucketExpression + ">=" + from.getTime());
    rootCriteriaString.add(bucketExpression + "<" + to.getTime());
    groupByExpressions.add(bucketExpressionWithInterval);
    orderByExpressions.add(alias);
    headers.add(alias);
  }

  @Override
  public void visit(Criteria criteria) {
    final DruidQueryBuilder.RootCriteriaBuilder filterBuilder = new DruidQueryBuilder.RootCriteriaBuilder();
    criteria.accept(filterBuilder);
    final String criteriaString = filterBuilder.criteriaBuilder.toString();
    if (!criteriaString.equals("")) {
      rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
    }
  }

  @Override public void visitGroupBy(String groupByColumn) {
    groupByExpressions.add(getModifiedColumn(groupByColumn));
  }

  @Override
  public void visit(Optional<Integer> limit) {
    this.limit = limit;
  }

  @Override
  public void visitFrom(String fromTable) {
    this.from = fromTable;
  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    orderByExpressions.add("\"" + getModifiedColumn(orderByColumn) + "\" " + castedConfig.getOrderByType(type));
  }

  @Override
  protected Object buildQueryImpl() {
    final StringBuilder queryBuffer = new StringBuilder();

    queryBuffer.append("select ");

    List<String> updatedSelectExpresssions = selectExpressions;
    Joiner.on(",").appendTo(queryBuffer, updatedSelectExpresssions);
    queryBuffer.append(" from ").append(from);

    if (!rootCriteriaString.isEmpty()) {
      queryBuffer.append(" where ");
      Joiner.on(" AND ").appendTo(queryBuffer, rootCriteriaString);
    }

    if (!groupByExpressions.isEmpty()) {
      queryBuffer.append(" group by ");
      Joiner.on(",").appendTo(queryBuffer, groupByExpressions);
    }

    if (!orderByExpressions.isEmpty()) {
      queryBuffer.append(" order by ");
      Joiner.on(",").appendTo(queryBuffer, orderByExpressions);
    }

    if (limit.isPresent()) {
      queryBuffer
          .append(" limit ")
          .append(limit.get());
    }
    Map<String,Object> context = new HashMap<>();
    context.put(DruidDSLConfig.SQL_TIME_ZONE_KEY,DruidDSLConfig.getTimeZone());
    context.put(DruidDSLConfig.QUERY_TIMEOUT,DruidDSLConfig.getQueryTimeout());
    return new DruidQuery(queryBuffer.toString(),headers,context);
  }

  private String getModifiedColumn(String columnName){
    return columnName.split("\\.").length == 1? columnName : columnName.split("\\.")[1];
  }

  class RootCriteriaBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

    private final StringBuilder criteriaBuilder = new StringBuilder();

    @Override
    public CriteriaVisitor visit(Predicate predicate) {
      final PredicateNodeBuilder localBuilder = new PredicateNodeBuilder(predicate);
      predicate.accept(localBuilder);
      String predicateString = localBuilder.getNode();
      if (!(predicateString.equals(""))) {
        criteriaBuilder.append(predicateString);
      }
      return new DefaultCriteriaVisitor();
    }

    @Override
    public CriteriaVisitor visit(LogicalOp logicalOp) {

      criteriaBuilder.append("(");
      final List<String> criteriaNodes = Lists.newArrayList();
      criteriaNodes.add(
          " 1=1 "); // A dummy criteria if none of the criteria are valid (coz of invalid params)
      for (Criteria criteria : logicalOp.getCriteria()) {
        final DruidQueryBuilder.RootCriteriaBuilder filterBuilder = new DruidQueryBuilder.RootCriteriaBuilder();
        criteria.accept(filterBuilder);
        final String criteriaString = filterBuilder.criteriaBuilder.toString();
        if (!criteriaString.equals("")) {
          criteriaNodes.add(criteriaString);
        }
      }
      switch (logicalOp.getType()) {
        case NOT:
          criteriaBuilder.append("!(").append(criteriaNodes.get(0)).append(")");
          break;
        case AND:
        case OR:
          Joiner.on(" " + getLogicalOpString(logicalOp.getType()) + " ")
              .appendTo(criteriaBuilder, criteriaNodes);
          break;
        default:
          throw new UnsupportedOperationException(
              "There are not handlers for this logical operator" + logicalOp.getType());
      }
      criteriaBuilder.append(")");
      return new DefaultCriteriaVisitor();
    }
  }

  class PredicateNodeBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

    private final Predicate predicate;
    private String columnName;
    private List<Object> values = Lists.newArrayList();
    private boolean isParamValueMissing = false;

    public PredicateNodeBuilder(Predicate predicate) {
      this.predicate = predicate;
    }

    @Override
    public CriteriaVisitor visit(Exp expression) {

      if (expression instanceof CompositeColumnExp && columnName == null) {

        columnName = getModifiedColumn(((CompositeColumnExp) expression).convertToProperExpression(
            s ->s));

      } else if (expression instanceof ColumnExp && columnName == null) {
        columnName = getModifiedColumn(((ColumnExp) expression).evaluateAndGetColName(paramValues));
      }
      return this;
    }

    @Override
    public CriteriaVisitor visit(EvalExp expression) {
      if (expression instanceof LiteralEvalExp) {
        values.add(((LiteralEvalExp) expression).value);
      }
      return this;
    }

    @Override
    public CriteriaVisitor visit(Param param) {
      try {
        final Object value = param.getValue(paramValues);
        if (param.isMultiple) {
          values.addAll((Collection<?>) value);
        } else {
          values.add(value);
        }
      } catch (Exception e) {
        log.warn(String
            .format("Filter %s, is ignored in the query since the value is missing. ", param.name)
            + e.getMessage());
        isParamValueMissing = true;
      }
      return this;

    }

    private boolean HasNullOperator() {
      Predicate.Type type = predicate.getType(paramValues);
      if (type.equals(Predicate.Type.is_not_null) || type.equals(Predicate.Type.is_null)) {
        return true;
      }
      return false;
    }

    public String getNode() {
      if (isParamValueMissing && !HasNullOperator()) {
        return "";
      }
//            final ObjectNode node = jsonNodeFactory.objectNode();
      final StringBuilder predicateBuilder = new StringBuilder();
      if(predicate.getType(paramValues) != Type.native_filter)
        predicateBuilder.append("\"").append(getModifiedColumn(columnName))
          .append("\"")
          .append(" ")
          .append(getPredicateStringFor(predicate.getType(paramValues)));
      switch (predicate.getType(paramValues)) {
        case not_in:
        case in: {
          if (values.isEmpty()) {
            throw new RuntimeException(
                "At-least one value is expected for if in parameter is passed");
          }
          predicateBuilder.append("(");
          Iterable<Object> wrappedObjects = Iterables
              .transform(values, new Function<Object, Object>() {
                @Nullable
                @Override
                public Object apply(Object input) {
                  return getWrappedObject(input);
                }
              });
          Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
          predicateBuilder.append(")");
          break;
        }
        case native_filter:
          predicateBuilder.append(values.get(0));
          break;
        case eq:
        case neq:
        case like:
        case lt:
        case lte:
        case gt:
        case gte: {
          predicateBuilder.append(getWrappedObject(values.get(0)));
          break;
        }
        case is_null:
        case is_not_null:
          // no need to add values to query for nulls
          break;
        default:
          throw new UnsupportedOperationException(
              "There are no handlers for the predicate " + predicate.getType(paramValues));
      }
      return predicateBuilder.toString();
    }
  }
}
