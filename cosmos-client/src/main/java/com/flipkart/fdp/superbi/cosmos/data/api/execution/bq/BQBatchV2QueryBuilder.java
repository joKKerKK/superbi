package com.flipkart.fdp.superbi.cosmos.data.api.execution.bq;

import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import java.time.DayOfWeek;
import java.time.LocalDate;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.TIMESTAMP;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.UNIX_MILLIS;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previous;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.time.DateUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getColumnName;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig.getDefaultTimeZone;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.ARRAY_CONCAT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.AS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.CLOSE_BOX_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.CLOSE_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.DATE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GENERATE_DATE_ARRAY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.GENERATE_TIMESTAMP_ARRAY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.INTERVAL;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.MILLISECOND;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OPEN_BOX_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.OPEN_PARENTHESIS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.RANGE_BUCKET;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQUtils.quote;

@Slf4j
public class BQBatchV2QueryBuilder extends BQBatchQueryBuilder {

  public BQBatchV2QueryBuilder(DSQuery query, Map<String, String[]> paramValues,
      AbstractDSLConfig config,
      Map<String, String> federationProperties,
      Function<String, Map<String, String>> bqDimensionProvider,
      Function<String, DataSource> getDataSource) {
    super(query, paramValues, config, federationProperties, bqDimensionProvider, getDataSource);
  }

  @Override
  public void visit(SelectColumn.SimpleColumn column) {
    super.visit(column);
  }

  @Override
  public void visit(SelectColumn.Aggregation column, SelectColumn.AggregationOptions options) {
    super.visit(column,options);
  }

  @Override
  public void visitFrom(String fromTable) {
    super.visitFrom(fromTable);
  }

  @Override
  public void visit(Criteria criteria) {
    super.visit(criteria);
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    super.visitGroupBy(groupByColumn);
  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    super.visitOrderBy(orderByColumn,type);
  }

  @Override
  public void visit(com.google.common.base.Optional<Integer> limit) {
    super.visit(limit);
  }

  @Override
  public void visit(SelectColumn.ConditionalAggregation column) {
    super.visit(column);
  }

  @Override
  public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
    super.visitHistogram(alias,columnName,from,to,interval);
  }

  @Override
  public void visitDateRange(String column, Date start, Date end) {
    boolean dateTimeType = false;
    if((dateHistogramColumns.size()>0&&dateHistogramColumns.contains(column))||
        (datetimeCols.size()>0&&datetimeCols.contains(getColumnName(column)))){
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
      if (dateTimeType) {
        visitDateFilterForDateTime(dateTimeExpression, start, end);
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

  @Override
  public void visitDateHistogram(String alias, String columnName, Date from, Date to,
                                 long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {
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
          .append("PARSE_TIMESTAMP(\"%Y-%m-%d %H:%M:%S\", CAST(")
          .append(modifiedColumnName)
          .append("*10000 + ")
          .append(timeColumnName)
          .append(formDateHistogramExpression(start, end, intervalMs));
    } else {
      final Timestamp start = new Timestamp(from.getTime());
      final Timestamp end = new Timestamp(to.getTime());
      bucketExpression.append(OPEN_PARENTHESIS)
          .append(RANGE_BUCKET)
          .append(OPEN_PARENTHESIS)
          .append("PARSE_TIMESTAMP(\"%Y-%m-%d %H:%M:%S\", CAST(")
          .append(modifiedColumnName)
          .append(formDateHistogramExpression(start, end, intervalMs));
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
        .append("' , '")
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
        .append("PARSE_TIMESTAMP(\"%Y-%m-%d %H:%M:%S\", CAST(")
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
        "MONTH" : "WEEK";

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

  private StringBuilder visitDateFilterForDateTime(String columnNameOrExpression, Date start,
      Date end) {
    StringBuilder dateBuilder = new StringBuilder(columnNameOrExpression);
    Timestamp startTimestamp = new Timestamp(start.getTime());
    Timestamp endTimestamp = new Timestamp(end.getTime());
    dateBuilder.append(">=").append("DATETIME(timestamp('").append(startTimestamp).append("', '")
        .append(getDefaultTimeZone()).append("'),'").append(getDefaultTimeZone()).append("')");
    if (end != null) {
      dateBuilder
          .append(" and ")
          .append(columnNameOrExpression)
          .append("<")
          .append("DATETIME(timestamp('")
          .append(endTimestamp).append("', '").append(getDefaultTimeZone()).append("'),'")
          .append(getDefaultTimeZone()).append("')");
    }
    return dateBuilder;
  }

  @Override
  public void visit(DateRangePredicate dateRangePredicate) {
    super.visit(dateRangePredicate);
  }

  @Override
  protected Object buildQueryImpl() {
    return super.buildQueryImpl();
  }
}

