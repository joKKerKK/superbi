package com.flipkart.fdp.superbi.dsl.query.factory;

import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.CompositeColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.ExpEvaluators;
import com.flipkart.fdp.superbi.dsl.query.exp.GenericEvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.exp.ParamExp;
import com.flipkart.fdp.superbi.dsl.query.exp.SelectColumnExp;
import com.google.common.collect.ImmutableSet;
import java.util.Set;


/**
 * User: shashwat
 * Date: 24/01/14
 */
@HasFactoryMethod
public class ExprFactory {
  private ExprFactory() {
  }

  public static EvalExp LIT(Object s) {
    return new LiteralEvalExp(s);
  }

  public static EvalExp PARAM(String name, DataType dataType) {
    return PARAM(name, dataType, false);
  }

  public static ColumnExp COL(String s) {
    return new ColumnExp(s);
  }

  public static ColumnExp COMPOSITE_COL(String s) {
    return new CompositeColumnExp(s);
  }

  public static ColumnExp COL_PARAM(String s) {
    return new ColumnExp(new ParamExp(new Param(s, false, DataType.STRING)));
  }

  public static ParamExp PARAM(String s) {
    return new ParamExp(new Param(s, false, DataType.STRING));
  }

  public static OrderByExp ORDER_BY(ColumnExp colName, OrderByExp.Type type) {
    return new OrderByExp(colName, new LiteralEvalExp(type));
  }

  public static OrderByExp ORDER_BY(ColumnExp colName, ParamExp type) {
    return new OrderByExp(colName, type);
  }

  public static GenericEvalExp JS(String exp, DataType valueType, Set<Param> params) {
    return new GenericEvalExp(exp, valueType, params, ExpEvaluators.JS);
  }

  public static GenericEvalExp JS(String exp, DataType valueType) {
    return JS(exp, valueType, ImmutableSet.<Param>of());
  }

  public static EvalExp PARAM(String name, DataType dataType, boolean isMultiple) {
    return new ParamExp(new Param(name, isMultiple, dataType));
  }

  public static SelectColumnExp SEL_COL(String columnName) {
    return SEL_COL(columnName, SelectColumn.VISIBLE_DEFAULT,
        SelectColumn.SimpleColumn.DEFAULT_EXPRESSION_VALUE);
  }

  public static SelectColumnExp SEL_COL(String columnName, boolean visible,
      boolean isNativeExpression) {
    return new SelectColumnExp(
        new SelectColumn.SimpleColumn(columnName, columnName, isNativeExpression));
  }

  public static SelectColumnExp WITH_VISIBILITY(
      SelectColumnExp selectColumn, boolean visible) {
    selectColumn.selectColumn.setVisible(visible);
    return selectColumn;
  }

  /**
   * @deprecated replaced by replaced by
   * {@link #DATE_HISTOGRAM(String, EvalExp, EvalExp, EvalExp, EvalExp, EvalExp)}
   */
  @Deprecated
  public static SelectColumnExp DATE_HISTOGRAM(String columnName, EvalExp from,
      EvalExp to, EvalExp interval, EvalExp downsampleUnit) {
    return new SelectColumnExp(new SelectColumn.DateHistogram(
        columnName.replace(".", "_").concat("_alias"),
        new LiteralEvalExp("INSTANTANEOUS"),
        columnName,
        from,
        to,
        interval,
        downsampleUnit
    ));
  }

  public static SelectColumnExp DATE_HISTOGRAM(String columnName,
                                               EvalExp from, EvalExp to, EvalExp interval,
                                               EvalExp seriesType, EvalExp downsampleUnit) {
    return new SelectColumnExp(new SelectColumn.DateHistogram(
        columnName.replace(".", "_").concat("_alias"),
        seriesType,
        columnName,
        from,
        to,
        interval,
        downsampleUnit
    ));
  }

  public static SelectColumnExp HISTOGRAM(String columnName, EvalExp from, EvalExp to,
      EvalExp interval) {
    return new SelectColumnExp(new SelectColumn.Histogram(
        columnName.replace(".", "_").concat("_alias"),
        SelectColumn.Type.DATE_HISTOGRAM,
        columnName,
        from,
        to,
        interval
    ));
  }

  public static SelectColumnExp EXPR(SelectColumn.Expression.ExpressionType type,
      String expression) {
    return new SelectColumnExp(
        new SelectColumn.Expression(type, expression, expression));
  }

  public static SelectColumnExp EXPR(String expression) {
    return new SelectColumnExp(
        new SelectColumn.Expression(SelectColumn.Expression.ExpressionType.JS, expression,
            expression));
  }

  public static SelectColumnExp AGGR(String columnName, AggregationType type) {
    return new SelectColumnExp(
        new SelectColumn.Aggregation(columnName, type, type + "_" + columnName, OPTIONS(), false));
  }

  public static SelectColumnExp AGGR(String columnName, AggregationType type, boolean isNativeExpression) {
    return new SelectColumnExp(
        new SelectColumn.Aggregation(columnName, type, type + "_" + columnName, OPTIONS(), isNativeExpression));
  }

  public static SelectColumnExp AGGR(String columnName, AggregationType type,
      SelectColumn.AggregationOptionsExpr options, boolean isNativeExpression) {
    return new SelectColumnExp(
        new SelectColumn.Aggregation(columnName, type, type + "_" + columnName, options, isNativeExpression));
  }

  public static SelectColumnExp AGGR(String columnName, AggregationType type,
                                     SelectColumn.AggregationOptionsExpr options) {
    return new SelectColumnExp(
        new SelectColumn.Aggregation(columnName, type, type + "_" + columnName, options, false));
  }

  public static SelectColumn.AggregationOptionsExpr OPTIONS() {
    return new SelectColumn.AggregationOptionsExpr();
  }

  public static SelectColumn.AggregationOptionsExpr WITH_FRACTILE(EvalExp fractile) {
    return new SelectColumn.AggregationOptionsExpr().withFractile(fractile);
  }

  public static SelectColumnExp FRACTILE(String columnName, EvalExp fractile) {
    return new SelectColumnExp(
        new SelectColumn.Aggregation(columnName, AggregationType.FRACTILE,
            AggregationType.FRACTILE + "_" + columnName,
            OPTIONS().withFractile(fractile), false));
  }


  public static SelectColumnExp AGGR(String columnName, AggregationType type,
      Criteria criteria, boolean isNativeExpression) {
    return new SelectColumnExp(
        new SelectColumn.ConditionalAggregation(columnName, type, type + "_" + columnName,
            criteria, isNativeExpression));
  }

}
