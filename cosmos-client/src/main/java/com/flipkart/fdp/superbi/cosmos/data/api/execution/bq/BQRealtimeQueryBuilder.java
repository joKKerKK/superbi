package com.flipkart.fdp.superbi.cosmos.data.api.execution.bq;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getAggregationString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getColumnName;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getDefaultTimeZone;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getWrappedObject;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.ARRAY_CONCAT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.CLOSE_BOX_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.CLOSE_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.DATE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GENERATE_DATE_ARRAY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.INTERVAL;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.MONTH;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OPEN_BOX_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OPEN_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.RANGE_BUCKET;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.TIMESTAMP;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.UNIX_MILLIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.WEEK;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previous;

import com.flipkart.fdp.mmg.cosmos.api.TableEnhancementSTO;
import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.flipkart.fdp.mmg.cosmos.entities.Table;
import com.flipkart.fdp.superbi.cosmos.DataSourceUtil;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

@Slf4j
public class BQRealtimeQueryBuilder extends AbstractQueryBuilder {

  private List<String> selectExpressions = Lists.newArrayList();
  private final List<String> selectAliases = Lists.newArrayList();
  private String from;
  private java.util.Optional<TableEnhancementSTO> tableEnhancementOp;
  private final List<String> dateFilter = Lists.newArrayList();
  private final List<String> rootCriteriaString = Lists.newArrayList();
  private List<String> groupByExpressions = Lists.newArrayList();
  private List<String> orderByExpressions = Lists.newArrayList();
  private final List<String> histogramExpressions = Lists.newArrayList();
  private final List<String> histogramExpressionAliases = Lists.newArrayList();
  private final List<String> bucketExpressions = Lists.newArrayList();
  private final List<String> selectOnlyGroupByAndHistogramExpressions = Lists.newArrayList();
  private final List<String> selectOnlyGroupByAndHistogramExpressionsAliases = Lists.newArrayList();
  private Optional<Integer> limit = Optional.of(0);
  private final Map<String, String> federationProperties;
  private List<String> arrayTypeColumnsNamesList = Lists.newArrayList();
  private List<String> unnestColumnList = Lists.newArrayList();
  private final Function<String, DataSource> getDataSource;
  private final HashMap<String,String> nativeExpMap = new HashMap<>();
  private final boolean isView;

  public BQRealtimeQueryBuilder(DSQuery query, Map<String, String[]> paramValues,
      AbstractDSLConfig config,
      Map<String, String> federationProperties, Function<String, DataSource> getDataSource) {
    super(query, paramValues, config);
    this.federationProperties = federationProperties;
    this.getDataSource = getDataSource;
    this.isView = ((BQDSLConfig) config).isViews();
  }

  public java.util.Optional<String> getPartitionColumn() {
    return tableEnhancementOp
        .flatMap(tableEnhancement -> java.util.Optional.of(
            new ArrayList<>(tableEnhancement.getPartitionBy())))
        .flatMap(partitions -> {
          if (partitions.size() > 0) {
            return java.util.Optional.of(partitions.get(0));
          }
          return java.util.Optional.empty();
        });
  }

  public java.util.Optional<Iterable<String>> getClusterColumn() {
    return tableEnhancementOp
        .flatMap(tableEnhancement -> java.util.Optional.of(tableEnhancement.getClusterBy()));
  }

  public java.util.Optional<Iterable<String>> getOrderByColumn() {
    return tableEnhancementOp
        .flatMap(tableEnhancement -> java.util.Optional.of(tableEnhancement.getOrderBy()));
  }

  @Override
  public void visit(SelectColumn.SimpleColumn column) {
    if(column.isNativeExpression)
      nativeExpMap.put(column.colName,column.alias);
    if(selectAliases.contains("`"+column.getAlias()+"`"))return;
    selectExpressions.add(
        new StringBuffer()
            .append(column.isNativeExpression ? column.colName : getColumnName(column.colName))
            .append(" as `")
            .append(column.getAlias())
            .append("`")
            .toString()
    );
    selectAliases.add(column.getAlias());
  }

  @Override
  public void visit(SelectColumn.Aggregation column,
      SelectColumn.AggregationOptions options) {
    if(column.isNativeExpression)
      nativeExpMap.put(column.colName,column.alias);
    selectExpressions.add(
        new StringBuffer()
            .append(getAggregationString(column.aggregationType))
            .append("(" + (column.aggregationType.equals(
                AggregationType.DISTINCT_COUNT) ?
                "distinct " :
                ""))
            .append(column.isNativeExpression ? column.colName : getColumnName(column.colName))
            .append(")")
            .append(" as `")
            .append(column.getAlias())
            .append("`")
            .toString()
    );
  }

  @Override
  public void visitFrom(String fromTable) {
    DataSource dataSource = getDataSource.apply(fromTable);
    Table table = DataSourceUtil.getTable(dataSource);
    this.tableEnhancementOp = DataSourceUtil.getTableEnhancement(table);
    arrayTypeColumnsNamesList = DataSourceUtil.getArrayColumns(table);
    StringBuilder fromBuilder = new StringBuilder();
    if (federationProperties.containsKey("bq.project_id") && StringUtils.isNotBlank(
        federationProperties.get("bq.project_id"))) {
      fromBuilder.append(federationProperties.get("bq.project_id")).append(".");
    }
    fromBuilder.append(federationProperties.get("bq.dataset_name"))
        .append(".");
    String[] partsFrom = fromTable.split("\\.");
    fromBuilder.append(
        federationProperties.getOrDefault("bq.table_name", partsFrom[partsFrom.length - 1]));
    this.from = fromBuilder.toString();
  }

  @Override
  public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
    int buckets = (int) ((to - from) / interval);
    String quotedAlias = "`" + alias + "`";
    StringBuilder bucketExpression = new StringBuilder("(")
        .append("range_bucket(")
        .append(getColumnName(columnName))
        .append(" , GENERATE_ARRAY(")
        .append(from)
        .append(",")
        .append(to)
        .append(",")
        .append(interval)
        .append(" ))-1)");
    String histogramExpr =
        " " + from + "+" + interval + "*" + bucketExpression + " as " + quotedAlias + " ";
    visitSelectColForHistogram(histogramExpr, bucketExpression.toString(), quotedAlias, buckets);
  }

  @Override
  public void visitDateHistogram(String alias, String columnName, Date from, Date to,
                                 long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {
    String column = getColumnName(columnName);
    rootCriteriaString.add(visitDateFilter(column, from, to).toString());
    long fromTsMs = from.getTime();
    long toTsMs = to.getTime();
    int buckets = (int) ((toTsMs - fromTsMs) / intervalMs);
    String quotedAlias = "`" + alias + "`";
    final String bucketExpression = formBucketExpression(from, to, intervalMs,
            column, downSampleUnit);


    String selectHistogramExpr =
        " " + fromTsMs + "+" + intervalMs + "*" + bucketExpression + " as " + quotedAlias + " ";

    visitSelectColForHistogram(selectHistogramExpr, bucketExpression.toString(), quotedAlias,
        buckets, downSampleUnit);
  }

  private String formDateHistogramExpression(Timestamp start, Timestamp end, long intervalMs) {
    StringBuilder sb = new StringBuilder();
    sb.append(" , ")
        .append("GENERATE_TIMESTAMP_ARRAY")
        .append("('")
        .append(start)
        .append(" ")
        .append(getDefaultTimeZone())
        .append("' , '")
        .append(end)
        .append(" ")
        .append(getDefaultTimeZone())
        .append("' , INTERVAL ")
        .append(intervalMs)
        .append(" MILLISECOND))-1)");
    return sb.toString();
  }

  private String formBucketExpression(Date from, Date to,
                                      long intervalMs, String modifiedColumnName,
                                      SelectColumn.DownSampleUnit downSampleUnit) {
    StringBuilder bucketExpression = new StringBuilder();
    if (downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth) ||
        downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarWeek)) {
      return formDateHistogramExpressionForCalendar(from, to, modifiedColumnName, downSampleUnit);
    } else {
      final Timestamp start = new Timestamp(from.getTime());
      final Timestamp end = new Timestamp(to.getTime());
      bucketExpression.append("(")
          .append("range_bucket")
          .append("(")
          .append(modifiedColumnName)
          .append(formDateHistogramExpression(start, end, intervalMs));
    }
    return bucketExpression.toString();
  }

  private String formDateHistogramExpressionForCalendar(Date from, Date to, String modifiedColumnName,
                                                        SelectColumn.DownSampleUnit downSampleUnit) {
    StringBuilder sb = new StringBuilder();

    sb.append(UNIX_MILLIS)
        .append(OPEN_PARENTHESIS)
        .append(TIMESTAMP)
        .append(OPEN_PARENTHESIS)
        .append(formArrayExpressionForCalendar(from, to, downSampleUnit))
        .append(OPEN_BOX_PARENTHESIS)
        .append(OPEN_PARENTHESIS)
        .append(RANGE_BUCKET)
        .append(OPEN_PARENTHESIS)
        .append("DATE")
        .append(OPEN_PARENTHESIS)
        .append(modifiedColumnName)
        .append(" ,'Asia/Kolkata')")
        .append(CLOSE_PARENTHESIS)
        .append(" , ")
        .append(formArrayExpressionForCalendar(from, to, downSampleUnit))
        .append(CLOSE_PARENTHESIS)
        .append("-1")
        .append(CLOSE_PARENTHESIS)
        .append(CLOSE_BOX_PARENTHESIS)
        .append(" ,'Asia/Kolkata'")
        .append(CLOSE_PARENTHESIS)
        .append(CLOSE_PARENTHESIS)
    ;
    return sb.toString();
  }

  private String formArrayExpressionForCalendar(Date from, Date to,
                                                SelectColumn.DownSampleUnit downSampleUnit) {
    StringBuilder sb = new StringBuilder();
    final Timestamp start = new Timestamp(from.getTime());
    final Timestamp end = new Timestamp(to.getTime());
    final Timestamp arrayStart = new Timestamp(getArrayStart(from, downSampleUnit));
    final Timestamp arrayEnd = new Timestamp(getArrayEnd(to, downSampleUnit));
    final String downSampleBQ =
        (downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth)) ?
            MONTH : WEEK;

    sb.append(ARRAY_CONCAT)
        .append(OPEN_PARENTHESIS)
        .append(OPEN_BOX_PARENTHESIS)
        .append(formDateExpressionFromTimestamp(start))
        .append(CLOSE_BOX_PARENTHESIS)
        .append(", ")
        .append(GENERATE_DATE_ARRAY)
        .append(OPEN_PARENTHESIS)
        .append(formDateExpressionFromTimestamp(arrayStart))
        .append(" , ")
        .append(formDateExpressionFromTimestamp(arrayEnd))
        .append(" , ")
        .append(INTERVAL)
        .append(" 1 ")
        .append(downSampleBQ)
        .append(CLOSE_PARENTHESIS)
        .append(CLOSE_PARENTHESIS);

    return sb.toString();
  }

  private String formDateExpressionFromTimestamp(Timestamp timestamp) {
    StringBuilder sb = new StringBuilder();
    sb.append(DATE)
        .append(OPEN_PARENTHESIS)
        .append("'")
        .append(timestamp)
        .append("'")
        .append(", ")
        .append("'")
        .append(getDefaultTimeZone())
        .append("'")
        .append(CLOSE_PARENTHESIS);


    return sb.toString();
  }

  private long getArrayStart(Date from, SelectColumn.DownSampleUnit downSampleUnit) {
    if(downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth)) {
      Date startMonth = DateUtils.ceiling(from, Calendar.MONTH);
      return startMonth.getTime();
    } else {
      final LocalDate localDate = from.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      final LocalDate nextSunday = localDate.with(next(DayOfWeek.SUNDAY));
      Date nextSundayDate = Date.from(nextSunday.atStartOfDay(ZoneId.systemDefault()).toInstant());
      return nextSundayDate.getTime();
    }
  }

  private long getArrayEnd(Date to, SelectColumn.DownSampleUnit downSampleUnit) {
    if(downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth)) {
      Date endMonth = DateUtils.truncate(to, Calendar.MONTH);
      return endMonth.getTime();
    } else {
      final LocalDate localDate = to.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      final LocalDate pastSunday = localDate.with(previous(DayOfWeek.SUNDAY));
      Date pastSundayDate = Date.from(pastSunday.atStartOfDay(ZoneId.systemDefault()).toInstant());
      return pastSundayDate.getTime();
    }
  }

  private void visitSelectColForHistogram(String histogramExpr, String bucketExpression,
                                          String quotedAlias, int buckets,
                                          SelectColumn.DownSampleUnit downSampleUnit) {
    if (selectExpressions.contains(quotedAlias)) {
      selectExpressions.remove(quotedAlias);
      selectAliases.remove(quotedAlias);
    }
    selectExpressions.add(histogramExpr);
    // Alias of the bucket expression does not work in where clause for some reason
    if(!(downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth)
        || downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarWeek))){
      rootCriteriaString.add(bucketExpression + ">" + -1);
      rootCriteriaString.add(bucketExpression + "<= " + buckets);
    }
    histogramExpressionAliases.add(quotedAlias);
    histogramExpressions.add(quotedAlias);
    bucketExpressions.add(bucketExpression);

    selectAliases.add(quotedAlias);
    selectOnlyGroupByAndHistogramExpressions.add(histogramExpr);
    selectOnlyGroupByAndHistogramExpressionsAliases.add(quotedAlias);
  }

  private void visitSelectColForHistogram(String histogramExpr, String bucketExpression,
      String quotedAlias, int buckets) {
    if(selectExpressions.contains(quotedAlias)){
      selectExpressions.remove(quotedAlias);
      selectAliases.remove(quotedAlias);
    }
    selectExpressions.add(histogramExpr);
    // Alias of the bucket expression does not work in where clause for some reason
    rootCriteriaString.add(bucketExpression + ">" + -1);
    rootCriteriaString.add(bucketExpression + "<= " + buckets);
    histogramExpressionAliases.add(quotedAlias);
    histogramExpressions.add(quotedAlias);
    bucketExpressions.add(bucketExpression);

    selectAliases.add(quotedAlias);
    selectOnlyGroupByAndHistogramExpressions.add(histogramExpr);
    selectOnlyGroupByAndHistogramExpressionsAliases.add(quotedAlias);
  }

  @Override
  public void visit(Criteria criteria) {
    final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
    criteria.accept(filterBuilder);
    rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    String columnName = getColumnName(groupByColumn);
    if(nativeExpMap.containsKey(columnName)){
      groupByExpressions.add(nativeExpMap.get(columnName));
    } else {
      groupByExpressions.add(columnName);
    }
    if(arrayTypeColumnsNamesList.contains(columnName)){
      StringBuilder sb = new StringBuilder();
      sb.append("UNNEST")
          .append("(")
          .append(columnName)
          .append(") ")
          .append(columnName);
      unnestColumnList.add(sb.toString());
    }
  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    orderByExpressions.add(
        getColumnName(orderByColumn) + " " + BQDSLConfig.getOrderByType(type));
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


  @Override
  public void visit(Optional<Integer> limit) {
    this.limit = limit;
  }

  @Override
  public void visitDateRange(String column, Date start, Date end) {
    column = getColumnName(column);
    if (!isView && getPartitionColumn().isPresent() && column.equals(getPartitionColumn().get())) {
      dateFilter.add(visitDateFilter(column, start, end).toString());
    } else {
      rootCriteriaString.add(visitDateFilter(column, start, end).toString());
    }
  }

  public StringBuilder visitDateFilter(String column, Date start, Date end) {
    StringBuilder dateBuilder = new StringBuilder(column);
    Timestamp startTimestamp = new Timestamp(start.getTime());
    Timestamp endTimestamp = new Timestamp(end.getTime());
    dateBuilder.append(">=").append("timestamp('").append(startTimestamp).append("', '")
        .append(getDefaultTimeZone()).append("')");
    if (end != null) {
      dateBuilder
          .append(" and ")
          .append(column)
          .append("<")
          .append("timestamp('")
          .append(endTimestamp).append("', '").append(getDefaultTimeZone()).append("')");
    }
    return dateBuilder;
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

  private StringBuilder buildLatestView() {
    StringBuilder queryBuffer = new StringBuilder();

    queryBuffer.append("with latest as ("
        + "select *, row_number() over (");
    queryBuffer.append(" partition by ");

    queryBuffer.append(StringUtils.join(getClusterColumn().orElse(Collections.emptyList()), ','));

    queryBuffer.append(" order by ");

    java.util.Optional<Iterable<String>> orderByColumns = getOrderByColumn();

    List<StringBuilder> orderColumns = new ArrayList<>();

    if (orderByColumns.isPresent()){
      Iterator<String> orderColumnsIterator = orderByColumns.get().iterator();
      while (orderColumnsIterator.hasNext()){
        StringBuilder builder = new StringBuilder(orderColumnsIterator.next());
        builder.append(" desc");
        orderColumns.add(builder);
      }
    }

    orderColumns.add(new StringBuilder("_creation_timestamp desc"));

    queryBuffer.append(StringUtils.join(orderColumns, ','));

    queryBuffer.append(" ) as row_number from `");

    queryBuffer.append(from).append("`");

    if (dateFilter.size() != 0) {
      queryBuffer.append(" where (");
      Joiner.on(") and( ").appendTo(queryBuffer, dateFilter);
      queryBuffer.append("))");
    } else {
      queryBuffer.append(")");
    }
    return queryBuffer;
  }

  @Override
  protected Object buildQueryImpl() {
    if (isView){
      return buildForView();
    } else {
      return buildForLatestView();
    }

  }

  private Object buildForView() {
    final StringBuilder queryBuffer = new StringBuilder();

    queryBuffer.append("select ");
    Joiner.on(",").appendTo(queryBuffer, selectExpressions);

    queryBuffer.append(" from `").append(from).append("`");

    if(!unnestColumnList.isEmpty()){
      queryBuffer.append(" , ");
      Joiner.on(" , ").appendTo(queryBuffer, unnestColumnList);
    }

    if (!rootCriteriaString.isEmpty()) {
      queryBuffer.append(" ").append("where").append(" ");
      Joiner.on(" and ").appendTo(queryBuffer, rootCriteriaString);
    }

    List<String> allGroupByExpressions = getAllGroupbyExpressions();
    if (!allGroupByExpressions.isEmpty()) {
      queryBuffer.append(" group by ");
      Joiner.on(",").appendTo(queryBuffer, allGroupByExpressions);
    }

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

    // add formatter to beautify query
    return queryBuffer.toString();
  }

  private Object buildForLatestView() {
    final StringBuilder queryBuffer = buildLatestView();

    queryBuffer.append("select ");
    Joiner.on(",").appendTo(queryBuffer, selectExpressions);

    queryBuffer.append(" from ").append("latest");

    if(!unnestColumnList.isEmpty()){
      queryBuffer.append(" , ");
      Joiner.on(" , ").appendTo(queryBuffer, unnestColumnList);
    }

    queryBuffer.append(" where row_number=1");

    if (!rootCriteriaString.isEmpty()) {
      queryBuffer.append(" and ");
      Joiner.on(" and ").appendTo(queryBuffer, rootCriteriaString);
    }

    List<String> allGroupByExpressions = getAllGroupbyExpressions();
    if (!allGroupByExpressions.isEmpty()) {
      queryBuffer.append(" group by ");
      Joiner.on(",").appendTo(queryBuffer, allGroupByExpressions);
    }

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

    // add formatter to beautify query
    return queryBuffer.toString();
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
      if (expression instanceof ColumnExp && columnName == null) {
        columnName = getColumnName(((ColumnExp) expression).evaluateAndGetColName(paramValues));
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
          values.addAll((List<String>) value);
        } else {
          values.add(String.valueOf(value));
        }
      } catch (Exception e) {
        log.warn(String.format("Filter %s, is ignored in the query since the value is missing.",
            param.name) + e.getMessage());
        isParamValueMissing = true;
      }
      return this;

    }

    public String getNode() {
      if (isParamValueMissing) {
        return "";
      }
      final StringBuilder predicateBuilder = new StringBuilder();
      if (predicate.getType(paramValues) != Predicate.Type.native_filter
          && !arrayTypeColumnsNamesList.contains(columnName)) {
          predicateBuilder.append(columnName)
              .append(" ")
              .append(BQDSLConfig.getPredicateStringFor(
                  predicate.getType(paramValues)));
      }
      switch (predicate.getType(paramValues)) {
        case in: {
          if (values.size() == 0) {
            throw new RuntimeException("At-least one value is expected for if in parameter is "
                + "passed");
          }
          if(arrayTypeColumnsNamesList.contains(columnName)){
            Iterable<Object> wrappedObjects = Iterables.transform(values, new com.google.common.base.Function<Object,
                Object>() {
              @Nullable
              @Override
              public Object apply(Object input) {
                return getWrappedObject(input);
              }
            });

            predicateBuilder.append("ARRAY_LENGTH(ARRAY(select * from UNNEST([");
            Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
            predicateBuilder.append("]) INTERSECT DISTINCT (select * from UNNEST(")
                .append(columnName)
                .append(") as ")
                .append(columnName)
                .append(")))>0");
          } else {
            predicateBuilder.append(" (");
            Iterable<Object> wrappedObjects = Iterables.transform(values,
                new com.google.common.base.Function<Object,
                    Object>() {
                  @Nullable
                  @Override
                  public Object apply(Object input) {
                    return getWrappedObject(input);
                  }
                });
            Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
            predicateBuilder.append(")");
          }
          break;
        }
        case not_in: {
          if (values.size() == 0) {
            throw new RuntimeException("At-least one value is expected for if in parameter is "
                + "passed");
          }
          if(arrayTypeColumnsNamesList.contains(columnName)){
            Iterable<Object> wrappedObjects = Iterables.transform(values, new com.google.common.base.Function<Object,
                Object>() {
              @Nullable
              @Override
              public Object apply(Object input) {
                return getWrappedObject(input);
              }
            });

            predicateBuilder.append("ARRAY_LENGTH(ARRAY(select * from UNNEST([");
            Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
            predicateBuilder.append("]) INTERSECT DISTINCT (select * from UNNEST(")
                .append(columnName)
                .append(") as ")
                .append(columnName)
                .append(")))=0");
          } else if (arrayTypeColumnsNamesList.contains(columnName)) {
            predicateBuilder.append(columnName)
                .append(" ")
                .append(BQDSLConfig.getPredicateStringFor(
                    predicate.getType(paramValues)));
            predicateBuilder.append(" (");
            Iterable<Object> wrappedObjects = Iterables.transform(values,
                new com.google.common.base.Function<Object,
                    Object>() {
                  @Nullable
                  @Override
                  public Object apply(Object input) {
                    return getWrappedObject(input);
                  }
                });
            Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
            predicateBuilder.append(")");
          } else {
            predicateBuilder.append(" (");
            Iterable<Object> wrappedObjects = Iterables.transform(values,
                new com.google.common.base.Function<Object,
                    Object>() {
                  @Nullable
                  @Override
                  public Object apply(Object input) {
                    return getWrappedObject(input);
                  }
                });
            Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
            predicateBuilder.append(")");
          }
          break;
        }
        case native_filter:
          predicateBuilder.append(values.get(0));
          break;
        case eq: {
          if (arrayTypeColumnsNamesList.contains(columnName) && !selectAliases.contains(
              columnName)) {
            predicateBuilder.append("ARRAY_LENGTH(ARRAY(select * from UNNEST([")
                .append(getWrappedObject(values.get(0)))
                .append("]) INTERSECT DISTINCT (select * from UNNEST(")
                .append(columnName)
                .append(") as ")
                .append(columnName)
                .append(")))>0");
          } else {
            predicateBuilder.append(getWrappedObject(values.get(0)));
          }
          break;
        }
        case neq: {
          if (arrayTypeColumnsNamesList.contains(columnName) && !selectAliases.contains(
              columnName)) {
            predicateBuilder.append("ARRAY_LENGTH(ARRAY(select * from UNNEST([")
                .append(getWrappedObject(values.get(0)))
                .append("]) INTERSECT DISTINCT (select * from UNNEST(")
                .append(columnName)
                .append(") as ")
                .append(columnName)
                .append(")))=0");
          } else if (arrayTypeColumnsNamesList.contains(columnName)) {
            predicateBuilder.append(columnName)
                .append(" ")
                .append(BQDSLConfig.getPredicateStringFor(
                    predicate.getType(paramValues)));
            predicateBuilder.append(getWrappedObject(values.get(0)));
          } else {
            predicateBuilder.append(getWrappedObject(values.get(0)));
          }
          break;
        }
        case like:
        case lt:
        case lte:
        case gt:
        case gte:{
          predicateBuilder.append(getWrappedObject(values.get(0)));
          break;
        }
        case is_null: {
          if(arrayTypeColumnsNamesList.contains(columnName) && !selectAliases.contains(columnName)) {
            predicateBuilder.append("(ARRAY_LENGTH(")
                .append(columnName)
                .append(")")
                .append("is null OR ")
                .append("ARRAY_LENGTH(")
                .append(columnName)
                .append(") = 0 )");
          }
          break;
        }
        case is_not_null: {
          if (arrayTypeColumnsNamesList.contains(columnName) && !selectAliases.contains(
              columnName)) {
            predicateBuilder.append("ARRAY_LENGTH(")
                .append(columnName)
                .append(") > 0 ");
          }
          break;
        }
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
      final PredicateNodeBuilder localBuilder =
          new PredicateNodeBuilder(predicate);
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
      criteriaNodes.add(" 1=1 "); // A dummy criteria if none of the criteria are valid (coz of
      // invalid params)
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
          Joiner.on(" " + logicalOp.getType() + " ").appendTo(criteriaBuilder, criteriaNodes);
          break;
        default:
          throw new UnsupportedOperationException("There are not handlers for this logical "
              + "operator" + logicalOp.getType());
      }
      criteriaBuilder.append(")");
      return new DefaultCriteriaVisitor();
    }
  }
}
