package com.flipkart.fdp.superbi.core.util;

import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.DOWN_SAMPLE;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.DOWN_SAMPLE_UNIT;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.END;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.END_TIME_STAMP;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.FilterType;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.GroupByType;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.OPERATOR;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.ORDER;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.SERIES_TYPE;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.START;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.START_TIMESTAMP;
import static com.flipkart.fdp.superbi.core.api.query.PanelEntry.STEP;
import static com.flipkart.fdp.superbi.dsl.query.SelectColumn.SeriesType.INSTANTANEOUS;
import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.AND;
import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.PRED;
import static com.flipkart.fdp.superbi.dsl.query.factory.DateRangeFactory.DATE_RANGE;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.AGGR;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.COL;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.COL_PARAM;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.COMPOSITE_COL;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.DATE_HISTOGRAM;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.EXPR;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.HISTOGRAM;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.LIT;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.OPTIONS;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.ORDER_BY;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.PARAM;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.SEL_COL;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.WITH_VISIBILITY;
import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;

import com.flipkart.fdp.superbi.core.api.query.PanelEntry;
import com.flipkart.fdp.superbi.core.api.query.PanelEntry.MetaColumn;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.core.config.DataPrivilege;
import com.flipkart.fdp.superbi.core.config.DataPrivilege.LimitPriority;
import com.flipkart.fdp.superbi.dao.NativeExpressionDao;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.CompositeColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.exp.SelectColumnExp;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 09/07/19
 */

public class DSQueryBuilder {
  private List<Criteria>
      criteriaList = Lists.newArrayList();
  private List<SelectColumnExp> selectedColumns = Lists.newArrayList();
  private List<DateRangePredicate> dateRangePredicate = Lists.newArrayList();
  private List<ColumnExp> groupByColumns = Lists.newArrayList();
  private List<OrderByExp> orderByColumns = Lists.newArrayList();
  private static String fromTable;
  private int autIncCounter = 1;

  private static NativeExpressionDao nativeExpressionDao;

  @Getter
  private Optional<QueryAndParam> queryAndParam;

  private DSQueryBuilder(QueryPanel queryPanel, Optional<Integer> limit,
                         DataPrivilege dataPrivilege,Integer sourceLimit, NativeExpressionDao nativeExpressionDao) {
    buildQueryFor(queryPanel,  limit, dataPrivilege,sourceLimit, nativeExpressionDao);
  }

  public static DSQueryBuilder getFor(QueryPanel queryPanel, Optional<Integer> limit,
                                      DataPrivilege dataPrivilege,
                                      Integer sourceLimit, NativeExpressionDao nativeExpressionDao) {
    return new DSQueryBuilder(queryPanel, limit, dataPrivilege, sourceLimit, nativeExpressionDao);
  }

  public void buildQueryFor(QueryPanel queryPanel, Optional<Integer> limit,
                            DataPrivilege dataPrivilege,Integer sourceLimit,
                            NativeExpressionDao nativeExpressionDao) {
    if(queryPanel == null) {
      this.queryAndParam = Optional.absent();
      return;
    }

    this.nativeExpressionDao = nativeExpressionDao;
    queryPanel.clearDefaultParams();
    fromTable = queryPanel.getFromTable();

    processGroupByPanel(queryPanel.getGroupByColumns());
    processSelectPanel(queryPanel.getSelectColumns());
    processFilterPanel(queryPanel.getFilterColumns());

    try{
      Optional<Integer> finalLimit = getLimitForQuery(limit,Optional.of(queryPanel.getLimit()),
          Optional.fromNullable(dataPrivilege.getLimit()),Optional.fromNullable(sourceLimit),dataPrivilege.getLimitPriority());

      if(finalLimit.isPresent() && finalLimit.get().equals(-1)){
        finalLimit = Optional.absent();
      }

      List<SelectColumn> selectColumnList = convert(selectedColumns);

      DSQuery dsQuery = new DSQuery(buildCriteria(), selectColumnList, dateRangePredicate, groupByColumns,
          orderByColumns, finalLimit, Optional.<Integer>absent(), fromTable);

      this.queryAndParam = Optional.of(new QueryAndParam(dsQuery, queryPanel.getDefaultParams()));

    }catch (Exception e){
      throw new RuntimeException(MessageFormat.format("couldn't set limit due to {0}", e.getMessage()), e);
    }

  }

  public Optional<Integer> getLimitForQuery(Optional<Integer> clientLimit,Optional<Integer> queryLimit,
      Optional<Integer> configLimit,Optional<Integer> sourceLimit,LimitPriority limitPriority) {
    switch (limitPriority){
      case SOURCE:
        return getLimitForSource(sourceLimit,clientLimit,queryLimit,configLimit);
      case CLIENT :
        return getLimitForClient(clientLimit,queryLimit,configLimit);
      case REPORT:
        return getLimitForReport(queryLimit,configLimit);
      case CONFIG:
        return getLimitForConfig(configLimit);
      case MIN:
        return getMinLimit(clientLimit,queryLimit,configLimit);
      default:
        return getLimitForConfig(configLimit);
    }
  }

  private Optional<Integer> getLimitForSource(Optional<Integer> sourceLimit, Optional<Integer> clientLimit,
      Optional<Integer> queryLimit, Optional<Integer> configLimit) {
    return sourceLimit.isPresent() ? sourceLimit : getLimitForClient(clientLimit,queryLimit,configLimit);
  }

  private Optional<Integer> getMinLimit(Optional<Integer> clientLimit, Optional<Integer> queryLimit,
      Optional<Integer> configLimit) {
    int minClientLimit = clientLimit.isPresent() ? clientLimit.get() : Integer.MAX_VALUE;
    int minQueryLimit = queryLimit.isPresent() ? queryLimit.get() : Integer.MAX_VALUE;
    int minConfigLimit = !configLimit.isPresent() || configLimit.get().equals(-1) ? Integer.MAX_VALUE : configLimit.get();
    int minLimit = Math.min(minClientLimit,Math.min(minQueryLimit,minConfigLimit));
    return minLimit == Integer.MAX_VALUE ? Optional.absent() : Optional.of(minLimit);
  }

  private Optional<Integer> getLimitForClient(Optional<Integer> clientLimit, Optional<Integer> queryLimit, Optional<Integer> configLimit) {
    return clientLimit.isPresent() ? clientLimit : getLimitForReport(queryLimit,configLimit);
  }

  private Optional<Integer> getLimitForReport(Optional<Integer> queryLimit,
      Optional<Integer> configLimit) {
    return queryLimit.isPresent() ? queryLimit : getLimitForConfig(configLimit);
  }

  private Optional<Integer> getLimitForConfig(Optional<Integer> configLimit) {
    if(!configLimit.isPresent()){
      throw new RuntimeException("data limit property not set in config");
    }
    return configLimit.get().equals(-1) ? Optional.absent() : configLimit;
  }

  private void processSelectPanel(List<PanelEntry> entries)
  {
    for(PanelEntry entry : entries)
    {
      PanelEntry.SelectType type = Enum.valueOf(PanelEntry.SelectType.class, entry.getColumnType());
      SelectColumnExp selectColumn = null;
      String expressionOrCol = null;
      switch (type)
      {
        case SELECT_GENERIC:
        case SELECT_DIMENSION_COLUMN:
        case SELECT_DIMENSION_LEVEL:
          selectColumn = SEL_COL(getFQN(entry));
          break;
        case SELECT_EXPRESSION:
          switch (entry.getExprType())
          {
            case GENERIC:
              selectColumn = EXPR(entry.getExpr());
              break;

            case NATIVE:
              selectColumn = SEL_COL(entry.getExpr(), SelectColumn.SimpleColumn.VISIBLE_DEFAULT,
                  true);
              break;

            case IMPORT:
              selectColumn =
                  SEL_COL(nativeExpressionDao.getNativeExpression(entry.getAlias().get(), fromTable),
                  SelectColumn.SimpleColumn.VISIBLE_DEFAULT,
                  true);
          }
          break;
        case SELECT_AGGREGATED_EXPRESSION:
          expressionOrCol = entry.getExpr();
        case SELECT_AGGREGATION:
          if (expressionOrCol == null)
            expressionOrCol = getFQN(entry);
          SelectColumn.AggregationOptionsExpr options = OPTIONS();
          boolean isNativeExpression =
              (entry.getExprType() == PanelEntry.ExprType.NATIVE || entry.getExprType() == PanelEntry.ExprType.IMPORT );
          if(entry.getAggregator() == AggregationType.FRACTILE)
          {
            Preconditions.checkState(entry.getNthFractile()>0 && entry.getNthFractile()<=1.0, "fractile value should be > 0 and <= 1");
            options = options.withFractile(LIT(entry.getNthFractile()));

          }
          if(!Objects.isNull(entry.getCriteria()))
          {
            Optional<Predicate> pred = generatePredicate(entry.getCriteria());

            selectColumn = pred.isPresent() ?AGGR(expressionOrCol, entry.getAggregator(),
                pred.get(), isNativeExpression):
                AGGR(expressionOrCol, entry.getAggregator(), isNativeExpression);

          }
          else
            selectColumn = AGGR(expressionOrCol, entry.getAggregator(), options, isNativeExpression);

          break;
      }
      if(entry.getAlias().isPresent())
        selectColumn = selectColumn.as(entry.getAlias().get());
      selectColumn = WITH_VISIBILITY(selectColumn, entry.isVisible());
      selectedColumns.add(selectColumn);


      //Orderby should be dynamic on all select columns
      String orderByColumn = getAliasOrFQN(entry);

      String orderByParamName = asInternalParam(orderByColumn, ORDER);
      entry.putDefaultParam(orderByParamName, toArr(entry.getOrderBy()));

      orderByColumns.add(ORDER_BY(COL(orderByColumn),PARAM(orderByParamName)));
    }
  }

  private  void processGroupByPanel(List<PanelEntry> entries)
  {
    for(PanelEntry entry : entries)
    {
      GroupByType type = Enum.valueOf(GroupByType.class, entry.getColumnType());

      switch (type)
      {
        case GROUP_BY_GENERIC:
        case GROUP_BY_DIMENSION_LEVEL:
        case GROUP_BY_DIMENSION_COLUMN:
          processGroupByEntryAndAdd(entry);
          break;
        case GROUP_BY_EXPRESSION:
          String nativeExpression = (entry.getExprType() == PanelEntry.ExprType.IMPORT) ?
              nativeExpressionDao.getNativeExpression(entry.getAlias().get(),
                  fromTable) : entry.getExpr();
          groupByColumns.add(COL(nativeExpression));
          break;
        case DATE_HISTOGRAM:

          String fQColName = getFQN(entry);
          String fQName = entry.getFactCol().getParamName();
          String startTimeStampParam = fQName + "." + START_TIMESTAMP;
          String endTimeStampParam = fQName + "." + END_TIME_STAMP;
          String downSamplePram = fQName + "." + DOWN_SAMPLE;
          String downSampleUnitPram = fQName + "." + DOWN_SAMPLE_UNIT;

          entry.putDefaultParam(startTimeStampParam, toArr(entry.getDateRange().get(START_TIMESTAMP)));
          entry.putDefaultParam(endTimeStampParam, toArr(entry.getDateRange().get(END_TIME_STAMP)));
          entry.putDefaultParam(downSamplePram, toArr(entry.getDateRange().get(DOWN_SAMPLE)));
          String downSampleUnitValue = (entry.getDateRange().get(DOWN_SAMPLE_UNIT) != null) ? entry.getDateRange().get(DOWN_SAMPLE_UNIT) : SelectColumn.DownSampleUnit.Default.toString();
          entry.putDefaultParam(downSampleUnitPram, toArr(downSampleUnitValue));
          entry.putDefaultParam(SERIES_TYPE, toArr(INSTANTANEOUS));

          SelectColumnExp dateHistogram = DATE_HISTOGRAM(
              fQColName, PARAM(startTimeStampParam, DataType.DATETIME),
              PARAM(endTimeStampParam,DataType.DATETIME),
              PARAM(downSamplePram, DataType.LONG),
              PARAM(SERIES_TYPE), PARAM(downSampleUnitPram));

          if(entry.getAlias().isPresent())
            dateHistogram = dateHistogram.as(entry.getAlias().get());
          selectedColumns.add(dateHistogram);
          break;
        case METRIC_BUCKETING:

          DataType dataType = entry.type();

          SelectColumnExp histogram = HISTOGRAM(
              getFQN(entry), LIT(entry.getBucket().get(START)),
              LIT(entry.getBucket().get(END)),
              LIT(entry.getBucket().get(STEP)));

          if(entry.getAlias().isPresent())
            histogram = histogram.as(entry.getAlias().get());
          selectedColumns.add(histogram);;
          break;
      }
    }
  }

  private static String asInternalParam(String fqName, String internalParam)
  {
    return String.format("%s$%s$", fqName, internalParam);
  }

  private static String getFQN(PanelEntry entry)
  {
    return entry.getFactCol().getFQName();
  }

  private static String getAliasOrFQN(PanelEntry entry)
  {
    if(entry.getAlias().isPresent())
      return entry.getAlias().get();
    else return getFQN(entry);
  }

  private static Optional<Predicate> generatePredicate(PanelEntry entry)
  {
    Predicate.Type operator = entry.getOperator();
    List<EvalExp> valueExps = Lists.newArrayList();
    ColumnExp column = buildColumnExp(entry);

    Optional<Predicate> predicate = absent();
    FilterType type = Enum.valueOf(FilterType.class, entry.getColumnType());
    if(entry.isDynamic()) {
      String fqParamName = entry.getFactCol().getParamName();
      if(entry.isNativeFilterExpColType()) {
        //In Case of FILTER_EXPRESSION value is actually native expression.
        String nativeExpression = (entry.getExprType() == PanelEntry.ExprType.NATIVE) ?
            entry.getExpr() :
            nativeExpressionDao.getNativeExpression(entry.getAlias().get(),
                fromTable);
        entry.putDefaultParam(fqParamName, Collections.singletonList(nativeExpression));
      }
      else {
        entry.putDefaultParam(fqParamName, entry.getValues());
      }
      //set isMultiple=true because changing single-values(like) op to multi-valued op(in) at runtime is not
      //considering all passed values
      valueExps.add(PARAM(fqParamName, entry.type(), true));

      /* Parameters are always dynamic */
      String opParam = asInternalParam(fqParamName, OPERATOR);
      entry.putDefaultParam(opParam, toArr(operator));

      predicate = of(PRED(column, PARAM(opParam), valueExps.toArray(new EvalExp[0])));
    }
    else if(!entry.canIgnoreFilter()) //static filters should be omitted if values are not proper
    {
      if(entry.isNullOpFilter())
      {
        valueExps.add(LIT(null));
      }
      else if (entry.isNativeFilterExpColType()) {
        String nativeExpression = (entry.getExprType() == PanelEntry.ExprType.NATIVE) ?
            entry.getExpr() :
            nativeExpressionDao.getNativeExpression(entry.getAlias().get(), fromTable);
        valueExps.add(LIT(nativeExpression));
      }
      else
      {
        valueExps.addAll(Lists.transform(entry.getValues(), new Function<String, EvalExp>() {

          @Nullable
          @Override
          public EvalExp apply(String input) {
            return LIT(entry.type().toValue(input));
          }
        }));
      }

      predicate = of(PRED(column, operator, valueExps.toArray(new EvalExp[0])));
    }
    return predicate;
  }

  private static ColumnExp buildColumnExp(PanelEntry entry)
  {
    if(entry.isDateFilterExpColType())
    {
      String exprWithFQNames = entry.getExpr();
      for(Map.Entry<String, MetaColumn> operand : entry.getOperands().entrySet())
      {
        exprWithFQNames = exprWithFQNames.replace(operand.getKey(), CompositeColumnExp.COL_START_FMT + operand.getValue().getFactColumn().getFQName()
            + CompositeColumnExp.COL_END_FMT );
      }
      return  COMPOSITE_COL(exprWithFQNames);
    }
    return COL(getFQN(entry));
  }

  private void processGroupByEntryAndAdd(PanelEntry entry)
  {
    ColumnExp groupByCol;
    String groupByColName;
    String actualColName =  getFQN(entry);
    if(entry.isDynamic())
    {
      groupByColName = "groupByParam"+(autIncCounter++);
      groupByCol = COL_PARAM(groupByColName);
      entry.putDefaultParam(groupByColName, toArr(actualColName));
    }
    else
    {
      groupByColName = actualColName;
      groupByCol = COL(groupByColName);
    }
    groupByColumns.add(groupByCol);
  }

  private static List<SelectColumn> convert(List<SelectColumnExp> selectColumnExps)
  {
    Function t =  new Function<SelectColumnExp, SelectColumn>() {
      @Override
      public SelectColumn apply( SelectColumnExp input) {
        return input.selectColumn;
      }
    };
    List<SelectColumn> selectColumns = Lists.transform(selectColumnExps, t);
    return selectColumns;

  }

  private  void processFilterPanel(List<PanelEntry> entries)
  {
    for(PanelEntry entry : entries)
    {
      FilterType type = Enum.valueOf(FilterType.class, entry.getColumnType());
      switch (type)
      {
        case FILTER_DATE_RANGE:

          String fQName = entry.getFactCol().getParamName();
          String fQColName = getFQN(entry);

          ColumnExp dateRangeCol = COL(fQColName);
          EvalExp startTimeExp, endTimeExp;
          String startTime = entry.getDateRange().get(START_TIMESTAMP);
          String endTime = entry.getDateRange().get(END_TIME_STAMP);

          if(entry.isDynamic())
          {
            entry.putDefaultParam(fQName + "." + START_TIMESTAMP, toArr(startTime));
            entry.putDefaultParam(fQName + "." + END_TIME_STAMP, toArr(endTime));
            startTimeExp = PARAM(fQName+ "."+ START_TIMESTAMP, DataType.DATETIME);
            endTimeExp = PARAM(fQName+ "."+ END_TIME_STAMP, DataType.DATETIME);
          }
          else
          {
            startTimeExp = LIT(startTime);
            endTimeExp = LIT(endTime);
          }

          DateRangePredicate dateRange = DATE_RANGE(dateRangeCol, startTimeExp , endTimeExp);
          dateRangePredicate.add(dateRange);
          break;
        case FILTER_GENERIC:
        case FILTER_DIMENSION_LEVEL:
        case FILTER_DIMENSION_COLUMN:
        case FILTER_EXPRESSION:
          Optional<Predicate> predicate = generatePredicate(entry);
          if(predicate.isPresent())
            criteriaList.add(predicate.get());
          break;
      }
    }
  }

  private  Optional<Criteria> buildCriteria()
  {
    return (Optional<Criteria>)(criteriaList.isEmpty()?Optional.absent(): of(AND(criteriaList)));
  }

  public static <T>String[] toArr(T t)
  {
    // TODO verify if logic works
    if(t == null) {
      return new String[0];
    }

    String[] value;
    if(t instanceof Optional) {
      if(((Optional) t).isPresent()) {
        value = new String []{ String.valueOf(((Optional) t).get())};
      }else {
        value = new String[0];
      }
    }else {
      value = new String []{ String.valueOf(t)};
    }

    return value;
  }

  @Getter
  @AllArgsConstructor
  public  static class QueryAndParam
  {
    private final DSQuery query;
    private final Map<String,String[]> params;
  }
}
