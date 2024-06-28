package com.flipkart.fdp.superbi.cosmos.data.api.execution.bq;

import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.flipkart.fdp.superbi.cosmos.BQUsageType;
import com.flipkart.fdp.superbi.cosmos.DataSourceUtil;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.TableCatalogInfo;
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
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getAggregationString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getColumnName;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getPredicateStringFor;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getDefaultTimeZone;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getWrappedObject;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.ARRAY_CONCAT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.AS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.CLOSE_BOX_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.CLOSE_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.DATE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.DISTINCT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.FRACTILE_GROUP_BY_HOLDER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.FRACTILE_PARTITION_BY_HOLDER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.FROM;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GENERATE_ARRAY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GENERATE_DATE_ARRAY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GENERATE_TIMESTAMP_ARRAY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GROUP_BY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.INTERVAL;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.LIMIT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.MILLISECOND;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.MONTH;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OPEN_BOX_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OPEN_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.ORDER_BY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OVER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.PERCENTILE_CONT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.RANGE_BUCKET;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.SELECT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.TIMESTAMP;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.UNIX_MILLIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.WEEK;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.WHERE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.addParenthesis;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.getWrappedAlias;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.quote;
import static com.flipkart.fdp.superbi.cosmos.utils.Constants.HIVE;
import static com.flipkart.fdp.superbi.cosmos.utils.Constants.TABLE;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previous;

@Slf4j
public class BQBatchQueryBuilder extends AbstractQueryBuilder {

  protected final List<String> rootCriteriaString = Lists.newArrayList();

  protected final List<String> groupByExpressions = Lists.newArrayList();
  protected final List<String> orderByExpressions = Lists.newArrayList();
  protected final List<String> histogramExpressions = Lists.newArrayList();
  protected final List<String> bucketExpressions = Lists.newArrayList();
  protected final List<String> histogramExpressionAliases = Lists.newArrayList();
  protected final List<String> joinExpressions = Lists.newArrayList();
  protected final List<String> fractileExpressions = Lists.newArrayList();
  protected final List<String> fractileAliases = Lists.newArrayList();
  protected final List<String> selectExpressions = Lists.newArrayList();
  protected final List<String> selectAliases = Lists.newArrayList();
  protected final List<String> selectOnlyGroupByAndHistogramExpressions = Lists.newArrayList();
  protected final List<String> selectOnlyGroupByAndHistogramExpressionsAliases = Lists.newArrayList();
  protected final HashMap<String,String> fromTableAlias = new HashMap<>();
  protected final List<String> nativeExp = Lists.newArrayList();
  protected final HashMap<String,String> nativeExpMap = new HashMap<>();
  protected List<String> datetimeCols = Lists.newArrayList();

  protected final BQDSLConfig castedConfig;
  protected final Map<String, String> federationProperties;
  protected final Function<String, Map<String, String>> bqDimensionProvider;
  protected final Function<String, DataSource> getDataSource;

  protected com.google.common.base.Optional<Integer> limit = com.google.common.base.Optional.of(0);
  protected String from;
  protected String factName;
  protected List<String> dateHistogramColumns;
  private BadgerClient badgerClient;
  private boolean isBadgerEnabled;

  public BQBatchQueryBuilder(DSQuery query, Map<String, String[]> paramValues,
      AbstractDSLConfig config,
      Map<String, String> federationProperties,
      Function<String, Map<String, String>> bqDimensionProvider,
      Function<String, DataSource> getDataSource) {
    super(query, paramValues, config);
    castedConfig = (BQDSLConfig) config;
    this.federationProperties = federationProperties;
    this.bqDimensionProvider = bqDimensionProvider;
    this.dateHistogramColumns = getDateTimeColumnsForHist(query);
    this.getDataSource = getDataSource;
    this.badgerClient = badgerClient;
    this.isBadgerEnabled =  ((BQDSLConfig) config).getIsBadgeEnabled();
  }

  private List<String> getDateTimeColumnsForHist(DSQuery query) {
    List<String> dateHistogramColumns = new ArrayList<>();
    for(SelectColumn column : query.getSelectedColumns()){
      if(column.type.name().equals("DATE_HISTOGRAM")){
        dateHistogramColumns.add(column.getName());
      }
    }
    return dateHistogramColumns;
  }

  @Override
  public void visit(SelectColumn.SimpleColumn column) {
    if(selectAliases.contains("`"+column.getAlias()+"`"))return;
    boolean isNative = column.isNativeExpression;
    String expression = column.colName;
    String updatedExpression = column.colName;
    if(isNative) {
      updatedExpression = BQUtils.rectifyNativeExpressions(expression);
      if(castedConfig.getUsageType().equals(BQUsageType.BQ_BATCH_V2)) {
        updatedExpression = BQUtils.replaceTimestampWithDatetime(updatedExpression);
      }
      nativeExp.add(expression);
      nativeExpMap.put(updatedExpression,column.alias);
    }
    String selectExpression = new StringBuffer()
        .append(column.isNativeExpression ? updatedExpression : getModifiedColumnName(updatedExpression))
        .append(getWrappedAlias(column.getAlias()))
        .toString();
    selectExpressions.add(selectExpression);
    visitSelectColumnForFractile(column, selectExpression);
  }

  @Override
  public void visit(SelectColumn.Aggregation column, SelectColumn.AggregationOptions options) {
    boolean isNative = column.isNativeExpression;
    if(isNative) {
      nativeExp.add(column.colName);
      String updatedExpression = BQUtils.rectifyNativeExpressions(column.colName);
      if(castedConfig.getUsageType().equals(BQUsageType.BQ_BATCH_V2)) {
        updatedExpression = BQUtils.replaceTimestampWithDatetime(updatedExpression);
      }
      nativeExpMap.put(updatedExpression,column.getAlias());
    }
    if (column.aggregationType == AggregationType.FRACTILE) {
      String fractileExpr =
          PERCENTILE_CONT + addParenthesis(getModifiedColumnName(column.colName) + " , "
              + options.fractile.get()) +" "+ OVER +" "+ addParenthesis(FRACTILE_PARTITION_BY_HOLDER)
              + getWrappedAlias(column.getAlias());
      fractileExpressions.add(fractileExpr);
      selectExpressions.add(fractileExpr);
    } else {
      selectExpressions.add(
          new StringBuffer()
              .append(getAggregationString(column.aggregationType))
              .append(OPEN_PARENTHESIS)
              .append(column.aggregationType.equals(
                  AggregationType.DISTINCT_COUNT) ?
                  DISTINCT:"")
              .append(getModifiedColumnName(column.colName))
              .append(CLOSE_PARENTHESIS)
              .append(getWrappedAlias(column.getAlias()))
              .toString()
      );
    }
    visitSelectColumnForFractile(column, options);
  }

  @Override
  public void visitFrom(String fromTable) {
    StringBuilder fromBuilder = new StringBuilder();
    String[] partsFrom = fromTable.split("\\.");
    this.factName = partsFrom.length == 1 ? MetaAccessor.get().getFactByName(fromTable).getTableName()
            : fromTable;
    if (federationProperties.containsKey("bq.project_id") && StringUtils.isNotBlank(
            federationProperties.get("bq.project_id"))) {
      fromBuilder.append(federationProperties.get("bq.project_id")).append(".");
    }
    String[] partsFact = this.factName.split("\\.");
    fromBuilder.append(federationProperties.getOrDefault("bq.dataset_name",partsFact[0]))
            .append(".");
    String tableName =
            federationProperties.getOrDefault("bq.table_name", partsFact[partsFact.length - 1]);
    if( isBadgerEnabled ) {
      tableName = getOrgNsOfFactFromBadger(factName);
    }
    fromBuilder.append(tableName);
    this.from = fromBuilder.toString();
    fromTableAlias.put(this.from,tableName);
    DataSource dataSource = getDataSource.apply(fromTable);
    this.datetimeCols = DataSourceUtil.getDateTimeColsList(dataSource);
  }

  private String getOrgNsOfFactFromBadger(String factName) {
    TableCatalogInfo tableCatalogInfo = badgerClient.getTableCatalogInfo(factName, HIVE, TABLE).get(0);
    return String.format("%s__%s",tableCatalogInfo.getDatabaseName(), tableCatalogInfo.getTableName());
  }

  @Override
  public void visit(Criteria criteria) {
    final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
    criteria.accept(filterBuilder);
    rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    String modifiedColName = getModifiedColumnName(groupByColumn);
    if(nativeExpMap.containsKey(modifiedColName)){
      groupByExpressions.add(nativeExpMap.get(modifiedColName));
    } else {
      groupByExpressions.add(modifiedColName);
    }
  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    orderByExpressions.add(quote(orderByColumn )+ " " + BQDSLConfig.getOrderByType(type));
  }

  @Override
  public void visit(com.google.common.base.Optional<Integer> limit) {
    this.limit = limit;
  }

  protected String getModifiedColumnName(String dimColExpr) {
    String[] parts = dimColExpr.split("\\.");
    if (parts.length == 4) {
      final String factForeignKey = parts[0];
      final String dimensionName = parts[1];
      final Dimension dim = MetaAccessor.get().getDimensionByName(dimensionName);
      Map<String,String> dimProps = bqDimensionProvider.apply(dim.getTableName());
      StringBuilder dimensionTableFullName = new StringBuilder();
      if (dimProps.containsKey("bq.project_id") && StringUtils.isNotBlank(
          dimProps.get("bq.project_id"))) {
        dimensionTableFullName.append(dimProps.get("bq.project_id")).append(".");
      }
      String[] tableNameParts = dim.getTableName().split("\\.");
      dimensionTableFullName.append(dimProps.getOrDefault("bq.dataset_name",tableNameParts[0]))
          .append(".")
          .append(dimProps.getOrDefault("bq.table_name",tableNameParts[1]));
      final String dimensionTableName = quote(dimensionTableFullName.toString());
      final String dimPk = dimensionName + "_key";
      final String dimensionTableAlias = parts[0] + "_" + parts[1];
      final String joinExpression = " left outer join "
          + dimensionTableName + getWrappedAlias(dimensionTableAlias) + " on "
          + dimensionTableAlias + "."
          + dimPk + " = " + fromTableAlias.get(from) + "." + factForeignKey;
      if (!joinExpressions.contains(joinExpression)) {
        joinExpressions.add(joinExpression);
      }
      return dimensionTableAlias + "." + parts[3];
    } else {
      if(nativeExp.contains(dimColExpr)) {
        String rectified = BQUtils.rectifyNativeExpressions(dimColExpr);
        if(castedConfig.getUsageType().equals(BQUsageType.BQ_BATCH_V2)) {
          return BQUtils.replaceTimestampWithDatetime(rectified);
        }
        return rectified;
      }
      return fromTableAlias.get(from)+"."+parts[parts.length-1];
    }
  }

  @Override
  public void visit(SelectColumn.ConditionalAggregation column) {
    final RootCriteriaBuilder caseCriteriaBuilder = new RootCriteriaBuilder();
    column.criteria.accept(caseCriteriaBuilder);
    final String caseExpression = " CASE WHEN "
        + caseCriteriaBuilder.criteriaBuilder
        + " THEN " + getModifiedColumnName(column.colName) + " END";
    selectExpressions.add(
        new StringBuffer()
            .append(column.type)
            .append(addParenthesis(caseExpression))
            .append(getWrappedAlias(column.getAlias()))
            .toString()
    );
    visitSelectColumnForFractile(column);
  }

  private void visitSelectColumnForFractile(SelectColumn col, Object... extras) {
    switch (col.type) {
      case SIMPLE:
        SelectColumn.SimpleColumn simple = (SelectColumn.SimpleColumn) col;
        //If it is a native expr and the same expr is used in groupby also, then add to fractile processing lists, otherwise ignore it
        if (simple.isNativeExpression && !hasGroupByExpr(simple.colName)) {
          break;
        }
        String selectCol = new StringBuffer()
            .append(getModifiedColumnName(simple.colName))
            .append(getWrappedAlias(simple.getAlias()))
            .toString();
        selectOnlyGroupByAndHistogramExpressions.add(selectCol);
        selectOnlyGroupByAndHistogramExpressionsAliases.add(quote(simple.getAlias()));
        break;
      case AGGREGATION:
        SelectColumn.Aggregation aggr = (SelectColumn.Aggregation) col;
        if (aggr.aggregationType == AggregationType.FRACTILE) {
          fractileAliases.add(aggr.getAlias());
        }
        break;
    }
    selectAliases.add(col.getAlias());
  }

  private boolean hasGroupByExpr(String expr) {
    return query.getSchema(paramValues).groupedBy.stream().anyMatch(expr::equals);
  }

  @Override
  public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
    String modifiedColumnName = getModifiedColumnName(columnName);
    int buckets = (int) ((to - from) / interval);
    String quotedAlias = quote(alias);
    StringBuilder bucketExpression = new StringBuilder(OPEN_PARENTHESIS)
        .append(RANGE_BUCKET)
        .append(OPEN_PARENTHESIS)
        .append(modifiedColumnName)
        .append(" , ")
        .append(GENERATE_ARRAY)
        .append(OPEN_PARENTHESIS)
        .append(from)
        .append(",")
        .append(to)
        .append(",")
        .append(interval)
        .append(" ")
        .append(CLOSE_PARENTHESIS)
        .append(CLOSE_PARENTHESIS)
        .append("-1")
        .append(CLOSE_PARENTHESIS);
    String histogramExpr =
        " " + from + "+" + interval + "*" + bucketExpression + " " + AS + " " + quotedAlias + " ";
    visitSelectColForHistogram(histogramExpr,bucketExpression.toString(),quotedAlias,buckets,
        SelectColumn.DownSampleUnit.Default);
  }

  @Override
  public void visitDateRange(String column, Date start, Date end) {
    boolean dateTimeType = false;
    if((dateHistogramColumns.size()>0&&dateHistogramColumns.contains(column))
        ||(datetimeCols.size()>0&&datetimeCols.contains(getColumnName(column)))){
      dateTimeType = true;
    }
    StringBuilder dateRangeBuilder = new StringBuilder();
    String modifiedColumnName = getModifiedColumnName(column);
    Optional<String> timeColumnOptional = castedConfig
        .getMatchingTimeColumnIfPresent(modifiedColumnName, this.factName);
    if (timeColumnOptional.isPresent()) {
      String timeColumn = timeColumnOptional.get();
      String dateTimeExpression = castedConfig.getDateExpression(modifiedColumnName, timeColumn);
      String dateTimeSurrugatePattern = castedConfig.getDateTimeSurrugatePattern();
      if(dateTimeType){
        visitDateFilterForDateTime(dateTimeExpression,start,end);
      }
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

      if (dateTimeType) {
        dateRangeBuilder = visitDateFilterForDateTime(modifiedColumnName, start, end);
      } else {
        // we format the date variable to yyyyMMdd pattern and use it as an integer.
        DateTimeFormatter dtf = DateTimeFormat.forPattern(castedConfig.getDateSurrugatePattern());
        String startValue = dtf.print(start.getTime());
        dateRangeBuilder.append(modifiedColumnName);
        dateRangeBuilder.append(">=")
            .append(startValue);
        if (end != null) {
          String endValue = dtf.print(end.getTime());
          dateRangeBuilder.append(" and ")
              .append(modifiedColumnName)
              .append("<")
              .append(endValue);
        }
      }
    }
    rootCriteriaString.add(dateRangeBuilder.toString());
  }

  private StringBuilder visitDateFilterForDateTime(String columnNameOrExpression, Date start, Date end) {
    StringBuilder dateBuilder = new StringBuilder(columnNameOrExpression);
    Timestamp startTimestamp = new Timestamp(start.getTime());
    Timestamp endTimestamp = new Timestamp(end.getTime());
    dateBuilder.append(">=").append("timestamp('").append(startTimestamp).append("', '")
        .append(getDefaultTimeZone()).append("')");
    if (end != null) {
      dateBuilder
          .append(" and ")
          .append(columnNameOrExpression)
          .append("<")
          .append("timestamp('")
          .append(endTimestamp).append("', '").append(getDefaultTimeZone()).append("')");
    }
    return dateBuilder;
  }


  @Override
  public void visitDateHistogram(String alias, String columnName, Date from, Date to, long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {
    visitDateRange(columnName, from, to);
    String modifiedColumnName = getModifiedColumnName(columnName);
    long fromTsMs = from.getTime();
    long toTsMs = to.getTime();
    int buckets = (int) ((toTsMs - fromTsMs) / intervalMs);
    String quotedAlias = quote(alias);
    final Optional<String> timeColumnOptional = castedConfig
        .getMatchingTimeColumnIfPresent(modifiedColumnName, this.factName);
    final String bucketExpression = formBucketExpression(from, to, intervalMs,
        modifiedColumnName, timeColumnOptional, downSampleUnit);

    String selectHistogramExpr;
    if(downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth) ||
        downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarWeek)) {
      selectHistogramExpr = " " + bucketExpression + " " + AS + " " + quotedAlias
          + " ";
    } else {
      selectHistogramExpr =
          " " + fromTsMs + "+" + intervalMs + "*" + bucketExpression + " " + AS + " " + quotedAlias
              + " ";
    }

    visitSelectColForHistogram(selectHistogramExpr, bucketExpression, quotedAlias,
        buckets, downSampleUnit);

  }

  private String formBucketExpression(Date from, Date to,
                                      long intervalMs, String modifiedColumnName,
                                      Optional<String> timeColumnOptional, SelectColumn.DownSampleUnit downSampleUnit) {
    StringBuilder bucketExpression = new StringBuilder();
    if (downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarMonth) ||
        downSampleUnit.equals(SelectColumn.DownSampleUnit.CalendarWeek)) {
      return formDateHistogramExpressionForCalendar(from, to, modifiedColumnName, downSampleUnit);
    } else if (timeColumnOptional.isPresent()) {
      final String timeColumnName = timeColumnOptional.get();
      final Timestamp start = new Timestamp(from.getTime());
      final Timestamp end = new Timestamp(to.getTime());
      bucketExpression.append(OPEN_PARENTHESIS)
          .append(RANGE_BUCKET)
          .append(OPEN_PARENTHESIS)
          .append("PARSE_TIMESTAMP(\"%Y-%m-%d %H:%M:%S%Ez\", CAST(")
          .append(modifiedColumnName)
          .append("*10000 + ")
          .append(timeColumnName)
          .append(formDateHistogramExpression(start,end,intervalMs));
    } else {
      final Timestamp start = new Timestamp(from.getTime());
      final Timestamp end = new Timestamp(to.getTime());
      bucketExpression.append(OPEN_PARENTHESIS)
          .append(RANGE_BUCKET)
          .append(OPEN_PARENTHESIS)
          .append("PARSE_TIMESTAMP(\"%Y-%m-%d %H:%M:%S%Ez\", CAST(")
          .append(modifiedColumnName)
          .append(formDateHistogramExpression(start,end,intervalMs));
    }
    return bucketExpression.toString();
  }

  private String formDateHistogramExpression(Timestamp start, Timestamp end, long intervalMs) {
    StringBuilder sb = new StringBuilder();
    sb.append(" AS STRING),'Asia/Kolkata')")
        .append(" , ")
        .append(GENERATE_TIMESTAMP_ARRAY)
        .append(OPEN_PARENTHESIS)
        .append("'")
        .append(start)
        .append(" ")
        .append(getDefaultTimeZone())
        .append( "' , '" )
        .append(end)
        .append(" ")
        .append(getDefaultTimeZone())
        .append("' , ")
        .append(INTERVAL)
        .append(" ")
        .append(intervalMs)
        .append(" ")
        .append(MILLISECOND)
        .append(CLOSE_PARENTHESIS)
        .append(CLOSE_PARENTHESIS)
        .append("-1")
        .append(CLOSE_PARENTHESIS);
    return sb.toString();
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
        .append("PARSE_TIMESTAMP(\"%Y-%m-%d %H:%M:%S%Ez\", CAST(")
        .append(modifiedColumnName)
        .append(" AS STRING),'Asia/Kolkata')")
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
                                          String quotedAlias, int buckets, SelectColumn.DownSampleUnit downSampleUnit){
    if(selectExpressions.contains(quotedAlias)){
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
  protected Object buildQueryImpl() {
    if (hasFractileWithGroupBy()) {
      return buildQueryImplForFractileWithGroupByCase();
    }
    return buildQueryImplForNormalCase();
  }

  boolean isFractileExpr(String expr) {
    return expr.contains(FRACTILE_PARTITION_BY_HOLDER);
  }

  protected Object buildQueryImplForNormalCase() {
    final StringBuilder queryBuffer = new StringBuilder();
    queryBuffer.append(SELECT).append(" ");

    List<String> updatedSelectExpressions = selectExpressions;

    if (!fractileExpressions.isEmpty()) {
      if (hasFractileWithoutGroupBy()) {
        // todo limitation: when  there is no group bys, this fractile function cannot be used with other aggregate functions.
        //Here we remove partition section from fractile when it is used without group bys
        updatedSelectExpressions = selectExpressions.stream().map(
            selectExpr -> isFractileExpr(selectExpr) ? selectExpr
                .replace(FRACTILE_PARTITION_BY_HOLDER, "")
                : selectExpr).collect(Collectors.toList());
//        limit = com.google.common.base.Optional.of(1);
      } else if (hasFractileWithGroupBy()) {
        /* remove fractile expr from appearing in select itself  because it will be
           taken care(added) in fractile sub query.
         */
        updatedSelectExpressions = selectExpressions.stream()
            .filter(selectExpr -> !isFractileExpr(selectExpr)).collect(
                Collectors.toList());
      }
    }
    Joiner.on(",").appendTo(queryBuffer, updatedSelectExpressions);
    queryBuffer.append(" ")
        .append(FROM)
        .append(" ")
        .append(quote(from))
        .append(" ")
        .append(fromTableAlias.get(from));

    if (!joinExpressions.isEmpty()) {
      queryBuffer.append(" ");
      Joiner.on(" ").appendTo(queryBuffer, joinExpressions);
    }

    if (!rootCriteriaString.isEmpty()) {
      queryBuffer.append(" ").append(WHERE).append(" ");
      Joiner.on(" AND ").appendTo(queryBuffer, rootCriteriaString);
    }
    List<String> allGroupByExpressions = getAllGroupbyExpressions();
    if (!allGroupByExpressions.isEmpty()) {
      queryBuffer.append(" ").append(GROUP_BY).append(" ");
      Joiner.on(",").appendTo(queryBuffer, allGroupByExpressions);
    }
    if (!hasFractileWithGroupBy()) { // order by and limit need to be applied only at the outer query level
      List<String> allOrderByExpressions = getAllOrderbyExpressions();
      if (!allOrderByExpressions.isEmpty()) {
        queryBuffer.append(" ").append(ORDER_BY).append(" ");
        Joiner.on(",").appendTo(queryBuffer, allOrderByExpressions);
      }
      if (limit.isPresent()) {
        queryBuffer.append(" ")
                .append(LIMIT)
                .append(" ")
                .append(limit.get());
      }
    }
    return queryBuffer.toString();
  }


  /*fractile is an analyitic function and not an aggregate in Vertica, so can not be used directly
  with Group Bys. work around : create sub query with fractile expressions with all where conditions
  and join the results with normal query' results
  */
  protected Object buildQueryImplForFractileWithGroupByCase() {

    final String AliasA = "A";
    final String AliasB = "B";

    final String primarySubQuery = (String) buildQueryImplForNormalCase();
    final StringBuilder fractileSubQuery = new StringBuilder();

    List<String> groupByExpressions = getAllGroupbyExpressions();

    List<String> allGroupByExpressions = groupByExpressions.stream()
        .map(s -> nativeExpMap.containsValue(s) ? nativeExpMapGetKey(nativeExpMap, s) : s)
        .collect(Collectors.toList());

    String combinedFractileExpr = commaJoin(fractileExpressions,
        f -> f.replace(FRACTILE_GROUP_BY_HOLDER,
            commaJoin(allGroupByExpressions)));

    String selectClause =
        SELECT+" "+ DISTINCT+ combinedFractileExpr + "," + commaJoin(
            selectOnlyGroupByAndHistogramExpressions);
    fractileSubQuery.append(selectClause);

    fractileSubQuery.append(" ")
        .append(FROM)
        .append(" ")
        .append(quote(from))
        .append(" ")
        .append(fromTableAlias.get(from));

    if (!joinExpressions.isEmpty()) {
      fractileSubQuery.append(" ");
      Joiner.on(" ").appendTo(fractileSubQuery, joinExpressions);
    }

    if (!rootCriteriaString.isEmpty()) {
      fractileSubQuery.append(" ").append(WHERE).append(" ");
      Joiner.on(" AND ").appendTo(fractileSubQuery, rootCriteriaString);
    }

    String outerSelectAliases = applyAliasForCommonColumns(selectAliases,
        selectOnlyGroupByAndHistogramExpressionsAliases, AliasA,true);

    String outerQuery = SELECT+" "
        + outerSelectAliases
        +" "+ FROM+ " "
        +addParenthesis(primarySubQuery)
        +" " + AliasA
        +" join "
        +addParenthesis(fractileSubQuery.toString())
        + " " + AliasB +
        " on " + buildJoinConditions(selectOnlyGroupByAndHistogramExpressionsAliases, AliasA,
        AliasB);

    List<String> allOrderByExpressions = getAllOrderbyExpressions();
    if (!allOrderByExpressions.isEmpty()) {
      outerQuery += " "+ ORDER_BY +" ";
      outerQuery += applyAliasForCommonColumns(allOrderByExpressions,
          selectOnlyGroupByAndHistogramExpressionsAliases, AliasA,false);
    }

    if (limit.isPresent()) {
      outerQuery += " "+LIMIT+" " + limit.get();
    }
    return outerQuery;
  }

  private static <K, V> K nativeExpMapGetKey(Map<K, V> map, V value)
  {
    return map.keySet()
        .stream()
        .filter(key -> value.equals(map.get(key)))
        .findFirst().get();
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

  private boolean hasFractileWithoutGroupBy() {
    return groupByExpressions.isEmpty() && histogramExpressions.isEmpty()
        && !fractileExpressions.isEmpty();
  }

  private boolean hasFractileWithGroupBy() {
    return !(groupByExpressions.isEmpty() && histogramExpressions.isEmpty())
        && !fractileExpressions.isEmpty();
  }

  private static String commaJoin(List<String> list) {
    return Joiner.on(",").join(list);
  }

  private static String commaJoin(List<String> list,
      Function<String, String> converter) {
    return list.stream().map(converter).collect(Collectors.joining(","));
  }

  private static String buildJoinConditions(List<String> cols, String alias1,
      String alias2) {
    return cols.stream()
        .map(col -> alias1 + "." + col + " = " + alias2 + "." + col)
        .collect(Collectors.joining(" AND "));
  }

  private static String applyAliasForCommonColumns(List<String> allColumns,
      List<String> commonColumns, String aliasToApply, boolean applyQuote) {
    return allColumns.stream().map(c -> {
      c = applyQuote ? quote(c) : c;
      c = commonColumns.contains(c) ? aliasToApply + "." + c : c;
      return c;
    }).collect(Collectors.joining(","));
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
      if (predicate.getType(paramValues) != Type.native_filter) {
        predicateBuilder
                .append(columnName)
                .append(" ")
                .append(getPredicateStringFor(predicate.getType(paramValues)));
      }
      switch (predicate.getType(paramValues)) {
        case in:
        case not_in: {
          if (values.size() == 0) {
            throw new RuntimeException("At-least one value is expected for if in parameter is "
                + "passed");
          }
          predicateBuilder.append(" ").append(OPEN_PARENTHESIS);
          Iterable<Object> wrappedObjects = Iterables.transform(values, new com.google.common.base.Function<Object,
              Object>() {
            @Nullable
            @Override
            public Object apply(Object input) {
              return getWrappedObject(input);
            }
          });
          Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
          predicateBuilder.append(CLOSE_PARENTHESIS);
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
      criteriaBuilder.append(OPEN_PARENTHESIS);
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
          criteriaBuilder.append("!").append(addParenthesis(criteriaNodes.get(0)));
          break;
        case AND:
        case OR:
          Joiner.on(" " + logicalOp.getType() + " ").appendTo(criteriaBuilder, criteriaNodes);
          break;
        default:
          throw new UnsupportedOperationException("There are not handlers for this logical "
              + "operator" + logicalOp.getType());
      }
      criteriaBuilder.append(CLOSE_PARENTHESIS);
      return new DefaultCriteriaVisitor();
    }
  }
}
