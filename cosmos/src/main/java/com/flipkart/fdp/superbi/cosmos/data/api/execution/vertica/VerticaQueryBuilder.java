package com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig.getAggregationString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig.getLogicalOpString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig.getPredicateStringFor;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig.getWrappedObject;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.LoggerFactory;

public class VerticaQueryBuilder extends AbstractQueryBuilder {

  private static org.slf4j.Logger logger = LoggerFactory.getLogger(VerticaQueryBuilder.class);

  private List<String> selectExpressions = Lists.newArrayList();
  private String from;
  private final List<String> rootCriteriaString = Lists.newArrayList();
  private List<String> histogramExpressions = Lists.newArrayList();
  private List<String> bucketExpressions = Lists.newArrayList();
  private List<String> histogramExpressionAliases = Lists.newArrayList();
  private List<String> groupByExpressions = Lists.newArrayList();
  private List<String> orderByExpressions = Lists.newArrayList();
  private List<String> joinExpressions = Lists.newArrayList();


  private List<String> selectAliases = Lists.newArrayList();
  private List<String> selectOnlyGroupByAndHistogramExpressions = Lists.newArrayList();
  private List<String> selectOnlyGroupByAndHistogramExpressionsAliases = Lists.newArrayList();
  private final String FRACTILE_GROUPBY_HOLDER = "{{FRACTILE_GROUP_BYS}}";
  private final String FRACTILE_PARTITIONBY_HOLDER = "PARTITION BY " + FRACTILE_GROUPBY_HOLDER;
  private List<String> fractileExpressions = Lists.newArrayList();
  private List<String> fractileAliases = Lists.newArrayList();
  private Optional<Integer> limit = Optional.of(0);

  private final VerticaDSLConfig castedConfig;

  public VerticaQueryBuilder(DSQuery query, Map<String, String[]> values, VerticaDSLConfig config) {
    super(query, values, config);
    castedConfig = config;
  }

  @Override
  public void visit(SelectColumn.SimpleColumn column) {
    String selectExpression = new StringBuffer()
        .append(column.isNativeExpression ? column.colName : getModifiedColumnName(column.colName))
        .append(" as '")
        .append(column.getAlias())
        .append("'")
        .toString();
    selectExpressions.add(
        selectExpression);

    visitSelectColumnForFractile(column, selectExpression);
  }

  private String getModifiedColumnName(String dimColExpr) {
    String[] parts = dimColExpr.split("\\.");
    if (parts.length == 4) {
      final String factForeignKey = parts[0];
      final String dimensionName = parts[1];
      final Dimension dim = MetaAccessor.get().getDimensionByName(dimensionName);
      final String dimensionTableName = dim.getTableName();
      final String dimPk = dimensionName + "_key";
      final String dimensionTableAlias = parts[0] + "_" + parts[1];
      final String joinExpression = " left outer join "
          + dimensionTableName + " as " + dimensionTableAlias + " on "
          + dimensionTableAlias + "."
          + dimPk + " = " + from + "." + factForeignKey;
      if (!joinExpressions.contains(joinExpression)) {
        joinExpressions.add(joinExpression);
      }
      return dimensionTableAlias + "." + parts[3];
    } else {
      return dimColExpr;
    }
  }

  @Override
  public void visit(SelectColumn.Aggregation column,
      SelectColumn.AggregationOptions options) {

    if (column.aggregationType == AggregationType.FRACTILE) {
      String fractileExpr =
          "Percentile_cont( " + options.fractile.get() + ") within GROUP ( ORDER BY "
              + getModifiedColumnName(column.colName)
              + " ASC) OVER ( " + FRACTILE_PARTITIONBY_HOLDER + ") as '" + column.getAlias() + "'";
      fractileExpressions.add(fractileExpr);
      selectExpressions.add(fractileExpr);
    } else {
      selectExpressions.add(
          new StringBuffer()
              .append(getAggregationString(
                  column.aggregationType))
              .append("(" + (column.aggregationType.equals(
                  AggregationType.DISTINCT_COUNT) ?
                  "distinct " :
                  ""))
              .append(getModifiedColumnName(column.colName))
              .append(")")
              .append(" as '")
              .append(column.getAlias())
              .append("'")
              .toString()
      );
    }
    visitSelectColumnForFractile(column, options);
  }

  @Override
  public void visit(SelectColumn.ConditionalAggregation column) {
    final RootCriteriaBuilder caseCriteriaBuilder = new RootCriteriaBuilder();
    column.criteria.accept(caseCriteriaBuilder);
    final String caseExpression = " CASE WHEN "
        + caseCriteriaBuilder.criteriaBuilder.toString()
        + " THEN " + getModifiedColumnName(column.colName) + " END";
    selectExpressions.add(
        new StringBuffer()
            .append(column.type)
            .append("(")
            .append(caseExpression)
            .append(")")
            .append(" as '")
            .append(column.getAlias())
            .append("'")
            .toString()
    );
    visitSelectColumnForFractile(column);
  }

  @Override
  public void visitFrom(String fromTable) {
    String[] partsFrom = fromTable.split("\\.");
    this.from = partsFrom.length == 1 ? MetaAccessor.get().getFactByName(fromTable).getTableName()
        : fromTable;
  }

  @Override
  public void visit(DateRangePredicate dateRangePredicate) {
    final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
    dateRangePredicate.accept(filterBuilder);
    final String criteriaString = filterBuilder.criteriaBuilder.toString();
    if (!criteriaString.equals("")) {
      rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
    }
  }

  @Override
  public void visit(Criteria criteria) {
    final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
    criteria.accept(filterBuilder);
    final String criteriaString = filterBuilder.criteriaBuilder.toString();
    if (!criteriaString.equals("")) {
      rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
    }
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    groupByExpressions.add(getModifiedColumnName(groupByColumn));
  }

  @Override
  public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
    int buckets = (int) ((to - from) / interval);
    String quotedAlias = "\"" + alias + "\"";
    String bucketExpression =
        "(width_bucket(" + columnName + " ," + from + "," + to + "," + buckets + ") - 1)";
    String histogramExpr =
        " " + from + "+" + interval + "*" + bucketExpression + " as " + quotedAlias + " ";
    selectExpressions.add(histogramExpr);
    // Alias of the bucket expression does not work in where clause for some reason - figure out a way
    rootCriteriaString.add(bucketExpression + ">" + -1);
    rootCriteriaString.add(bucketExpression + "<" + buckets);
    histogramExpressionAliases.add(quotedAlias);
    histogramExpressions.add(bucketExpression);
    bucketExpressions.add(bucketExpression);

    selectAliases.add(quotedAlias);
    selectOnlyGroupByAndHistogramExpressions.add(histogramExpr);
    selectOnlyGroupByAndHistogramExpressionsAliases.add(quotedAlias);
  }


  @Override
  public void visitDateHistogram(String alias, String columnName, Date from, Date to,
                                 long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {

    visitDateRange(columnName, from, to);

    long fromTsMs = from.getTime();
    long toTsMs = to.getTime();
    int buckets = (int) ((toTsMs - fromTsMs) / intervalMs);
    String quotedAlias = "\"" + alias + "\"";
    final Optional<String> timeColumnOptional = castedConfig
        .getMatchingTimeColumnIfPresent(columnName, this.from);
    final String bucketExpression;
    if (timeColumnOptional.isPresent()) {
      final String dateTimeSurrugatePattern = castedConfig.getTimestampPattern();
      final String timeColumnName = timeColumnOptional.get();
      final DateTimeFormatter dtf = DateTimeFormat.forPattern(dateTimeSurrugatePattern);
      final String start = dtf.print(from.getTime());
      final String end = dtf.print(to.getTime());
      bucketExpression = "(width_bucket(to_timestamp(" + columnName + "*10000 + " + timeColumnName
          + ", 'YYYYMMDDHHMI'),'" + start + "','" + end + "'," + buckets + ")-1)";
    } else {
      final DateTimeFormatter dtf = DateTimeFormat.forPattern(castedConfig.getTimestampPattern());
      final String start = dtf.print(from.getTime());
      final String end = dtf.print(to.getTime());
      bucketExpression =
          "(width_bucket(to_timestamp(" + columnName + ", 'YYYYMMDD') ,'" + start + "','" + end
              + "'," + buckets + ")-1)";
    }

    String selectHistogramExpr =
        " " + fromTsMs + "+" + intervalMs + "*" + bucketExpression + " as " + quotedAlias + " ";

    selectExpressions.add(selectHistogramExpr);
    // Alias of the bucket expression does not work in where clause for some reason - figure out a way
    rootCriteriaString.add(bucketExpression + ">" + -1);
    rootCriteriaString.add(bucketExpression + "<" + buckets);
    histogramExpressionAliases.add(quotedAlias);
    histogramExpressions.add(bucketExpression);
    bucketExpressions.add(bucketExpression);

    selectAliases.add(quotedAlias);
    selectOnlyGroupByAndHistogramExpressions.add(selectHistogramExpr);
    selectOnlyGroupByAndHistogramExpressionsAliases.add(quotedAlias);

  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    orderByExpressions.add("\"" + orderByColumn + "\" " + castedConfig.getOrderByType(type));
  }


  @Override
  public void visit(Optional<Integer> limit) {
    this.limit = limit;
  }

  @Override
  public void visitDateRange(String column, Date start, Date end) {
    final StringBuilder dateRangeBuilder = new StringBuilder();
    Optional<String> timeColumnOptional = castedConfig
        .getMatchingTimeColumnIfPresent(column, this.from);
    if (timeColumnOptional.isPresent()) {
      String timeColumn = timeColumnOptional.get();
      String dateTimeExpression = castedConfig.getDateExpression(column, timeColumn);
      String dateTimeSurrugatePattern = castedConfig.getDateTimeSurrugatePattern();
      DateTimeFormatter dtf = DateTimeFormat.forPattern(dateTimeSurrugatePattern);
      String startValue = dtf.print(start.getTime());
      dateRangeBuilder.append(dateTimeExpression)
          .append(">=")
          .append(startValue);
      if (end != null) {
        String endValue = dtf.print(end.getTime());
        dateRangeBuilder.append(" and ")
            .append(dateTimeExpression)
            .append("<")
            .append(endValue);
      }
    } else {

      DateTimeFormatter dtf = DateTimeFormat.forPattern(castedConfig.getDateSurrugatePattern());
      String startValue = dtf.print(start.getTime());
      dateRangeBuilder.append(column);
      dateRangeBuilder.append(">=")
          .append(startValue);
      if (end != null) {
        String endValue = dtf.print(end.getTime());
        dateRangeBuilder.append(" and ")
            .append(column)
            .append("<")
            .append(endValue);
      }
    }
    rootCriteriaString.add(dateRangeBuilder.toString());
  }

  @Override
  protected Object buildQueryImpl() {
    if (hasFractileWithGroupBy()) {
      return buildQueryImplForFractileWithGroupByCase();
    } else {
      return buildQueryImplForNormalCase();

    }


  }

  boolean isFractileExpr(String expr) {
    return expr.contains(FRACTILE_PARTITIONBY_HOLDER);
  }

  protected Object buildQueryImplForNormalCase() {

    final StringBuilder queryBuffer = new StringBuilder();

    queryBuffer.append("select ");

    List<String> updatedSelectExpresssions = selectExpressions;

    if (!fractileExpressions.isEmpty()) {
      if (hasFractileWithoutGroupBy()) {
        // todo limitation: when  there is no group bys, this fractile function cannot be used with other aggregate functions.
        //Here we remove partition section from fractile when it is used without group bys
        updatedSelectExpresssions = selectExpressions.stream().map(
            selectExpr -> isFractileExpr(selectExpr) ? selectExpr
                .replace(FRACTILE_PARTITIONBY_HOLDER, "")
                : selectExpr).collect(Collectors.toList());
        limit = Optional.of(1);
      } else if (hasFractileWithGroupBy()) { // remove fractile expr from appearing in select itself  because it will be taken care(added) in fractile sub query.
        updatedSelectExpresssions = selectExpressions.stream()
            .filter(selectExpr -> !isFractileExpr(selectExpr)).collect(
                Collectors.toList());
      }


    }

    Joiner.on(",").appendTo(queryBuffer, updatedSelectExpresssions);

    queryBuffer.append(" from ").append(from);

    if (!joinExpressions.isEmpty()) {
      queryBuffer.append(" ");
      Joiner.on(" ").appendTo(queryBuffer, joinExpressions);
    }

    if (!rootCriteriaString.isEmpty()) {
      queryBuffer.append(" where ");
      Joiner.on(" AND ").appendTo(queryBuffer, rootCriteriaString);
    }

    List<String> allGroupByExprs = getAllGroupbyExpressions();

    if (!allGroupByExprs.isEmpty()) {
      queryBuffer.append(" group by ");
      Joiner.on(",").appendTo(queryBuffer, allGroupByExprs);
    }

    if (!hasFractileWithGroupBy()) { // order by and limit need to be applied only at the outer query level
      List<String> allOrderByExpressions = getAllOrderbyExpressions();
      if (!allOrderByExpressions.isEmpty()) {
        queryBuffer.append(" order by ");
        Joiner.on(",").appendTo(queryBuffer, allOrderByExpressions);
      }
      if (limit.isPresent()) {
        queryBuffer
            .append(" limit ")
            .append(limit.get());
      }
      System.out.println(queryBuffer.toString());
    }

    return queryBuffer.toString();
  }

  private List<String> getAllOrderbyExpressions() {
    List<String> list = Lists.newArrayList();
    list.addAll(histogramExpressionAliases);
    list.addAll(orderByExpressions);
    return list;
  }

  private List<String> getAllGroupbyExpressions() {
    List<String> list = Lists.newArrayList();
    list.addAll(histogramExpressions);
    list.addAll(groupByExpressions);
    return list;
  }

  private void visitSelectColumnForFractile(SelectColumn col,
      Object... extras) {
    switch (col.type) {
      case SIMPLE:
        SelectColumn.SimpleColumn simple = (SelectColumn.SimpleColumn) col;
        //If it is a native expr and the same expr is used in groupby also, then add to fractile processing lists, otherwise ignore it
        if (simple.isNativeExpression && !hasGroupByExpr(simple.colName)) {
          break;
        }
        String selectCol = new StringBuffer().append(
            getModifiedColumnName(simple.colName))
            .append(" as '")
            .append(simple.getAlias())
            .append("'")
            .toString();
        selectOnlyGroupByAndHistogramExpressions.add(selectCol);
        selectOnlyGroupByAndHistogramExpressionsAliases.add(
            quote(simple.getAlias()));

        break;
      case AGGREGATION:
        SelectColumn.Aggregation aggr = (SelectColumn.Aggregation) col;
        SelectColumn.AggregationOptions option = (SelectColumn.AggregationOptions) extras[0];
        if (aggr.aggregationType == AggregationType.FRACTILE) {
          fractileAliases.add(aggr.getAlias());
        }
        break;
    }
    selectAliases.add(col.getAlias());
  }

  private boolean hasGroupByExpr(String expr) {
    return query.getSchema(paramValues).groupedBy.stream().filter(expr::equals).findAny()
        .isPresent();
  }

  //fractile is an analyitic function and not an aggregate in Vertica, so can not be used directly with Group Bys.
//work around : create sub query with fractile expressions with all where conditions
// and join the results with normal query' results
  protected Object buildQueryImplForFractileWithGroupByCase() {

    final String AliasA = "A";
    final String AliasB = "B";

    final String primarySubQuery = (String) buildQueryImplForNormalCase();
    final StringBuilder fractileSubQuery = new StringBuilder();

    List<String> allGroupByExprs = getAllGroupbyExpressions();

    String combinedFractileExpr = commaJoin(fractileExpressions,
        f -> f.replace(FRACTILE_GROUPBY_HOLDER,
            commaJoin(allGroupByExprs)));

    String selectClause =
        "select distinct " + combinedFractileExpr + "," + commaJoin(
            selectOnlyGroupByAndHistogramExpressions);
    fractileSubQuery.append(selectClause);

    fractileSubQuery.append(" from ").append(from);

    if (!joinExpressions.isEmpty()) {
      fractileSubQuery.append(" ");
      Joiner.on(" ").appendTo(fractileSubQuery, joinExpressions);
    }

    if (!rootCriteriaString.isEmpty()) {
      fractileSubQuery.append(" where ");
      Joiner.on(" AND ").appendTo(fractileSubQuery, rootCriteriaString);
    }

    String outerSelectAliases = applyAliasForCommonColumns(selectAliases,
        selectOnlyGroupByAndHistogramExpressionsAliases, AliasA);

    String outerQuery = "select "
        + outerSelectAliases +
        " from "
        + "(" + primarySubQuery + ") " + AliasA +
        " join ("
        + fractileSubQuery.toString() + ") " + AliasB +
        " on " + buildJoinConditions(selectOnlyGroupByAndHistogramExpressionsAliases, AliasA,
        AliasB);

    List<String> allOrderByExpressions = getAllOrderbyExpressions();
    if (!allOrderByExpressions.isEmpty()) {
      outerQuery += " order by ";
      outerQuery += applyAliasForCommonColumns(allOrderByExpressions,
          selectOnlyGroupByAndHistogramExpressionsAliases, AliasA);
    }

    if (limit.isPresent()) {
      outerQuery += " limit " + limit.get();
    }
    System.out.println(outerQuery.toString());
    return outerQuery.toString();
  }

  private boolean hasFractileWithoutGroupBy() {
    return groupByExpressions.isEmpty() && histogramExpressions.isEmpty()
        && !fractileExpressions.isEmpty();
  }

  private boolean hasFractileWithGroupBy() {
    return (!groupByExpressions.isEmpty()
        || !histogramExpressions.isEmpty())
        && !fractileExpressions.isEmpty();
  }

  private static String commaJoin(List<String> list) {
    return Joiner.on(",").join(list);
  }

  private static String commaJoin(List<String> list,
      java.util.function.Function<String, String> converter) {
    return list.stream().map(converter).collect(Collectors.joining(","));
  }

  private static String buildJoinConditions(List<String> cols, String alias1,
      String alias2) {
    return cols.stream()
        .map(col -> alias1 + "." + col + " = " + alias2 + "." + col)
        .collect(Collectors.joining(" AND "));
  }

  private static String applyAliasForCommonColumns(List<String> allColumns,
      List<String> commonColumns, String aliasToApply) {
    return allColumns.stream().map(c -> {
      c = quote(c);
      c = commonColumns.contains(c) ? aliasToApply + "." + c : c;
      return c;

    }).collect(Collectors.joining(","));

  }

  private static String quote(String s) {
    return s.startsWith("\"") ? s : '"' + s + '"';
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

        columnName = ((CompositeColumnExp) expression).convertToProperExpression(
            s -> getModifiedColumnName(s));

      } else if (expression instanceof ColumnExp && columnName == null) {
        columnName = getModifiedColumnName(
            ((ColumnExp) expression).evaluateAndGetColName(paramValues));
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
        logger.warn(String
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
        predicateBuilder.append(columnName)
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
        final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
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

}
