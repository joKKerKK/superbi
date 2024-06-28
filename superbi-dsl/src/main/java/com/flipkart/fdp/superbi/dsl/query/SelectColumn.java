package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.Evaluable;
import com.flipkart.fdp.superbi.dsl.query.exp.ExprEvalException;
import com.flipkart.fdp.superbi.dsl.query.visitors.DSQueryVisitor;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * User: aniruddha.gangopadhyay
 * Date: 19/03/14
 * Time: 3:19 PM
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class SelectColumn implements Serializable{

    public abstract void accept(DSQueryVisitor visitor);

    public enum Type {SIMPLE, DERIVED, CONDITIONAL_AGGREGATION, AGGREGATION, DATE_HISTOGRAM, HISTOGRAM}

    public String alias;
    public final Type type;
    private DataType dataType;
    private boolean visible;
    public static final boolean VISIBLE_DEFAULT = true;


    public SelectColumn(String alias, Type type){
        this(alias,type, VISIBLE_DEFAULT);
    }
    public SelectColumn(String alias, Type type, boolean visible){
        this.alias = alias;
        this.type = type;
        this.visible = visible;
    }

    public DataType getDataType() {
        return dataType;
    }

    /*
    The setter is exposed here because the query layer is totally decoupled from the meta layer.
    The data types can be found out only after the execution
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    public static class F {
        public static Function<SelectColumn, String> name = new Function<SelectColumn, String>() {
            @Nullable
            @Override
            public String apply(@Nullable SelectColumn selectColumn) {
                return selectColumn.alias;
            }
        };

    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SelectColumn)) {
            return false;
        }
        SelectColumn that = (SelectColumn) o;
        return isVisible() == that.isVisible() &&
            Objects.equal(getAlias(), that.getAlias()) &&
            type == that.type &&
            getDataType() == that.getDataType();
    }

    public static class Expression extends SelectColumn {

        public static enum ExpressionType {JS}

        public final ExpressionType type;
        public final String expressionString;

        @JsonCreator
        public Expression(@JsonProperty("type") ExpressionType type,
            @JsonProperty("expressionString") String expressionString,
            @JsonProperty("alias") String alias) {
            super(alias, Type.DERIVED);
            this.expressionString = expressionString;
            this.type = type;
        }

        public static Function<Expression, String> expressions = new Function<Expression, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Expression expression) {
                return expression.expressionString;
            }
        };

        @Override
        public void accept(DSQueryVisitor visitor) {
            /*Currently expressions are handled purely in server side*/
            //visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Expression)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            Expression that = (Expression) o;
            return type == that.type &&
                Objects.equal(expressionString, that.expressionString);
        }
    }

    public static class SimpleColumn extends SelectColumn {

        public final String colName;
        public final boolean isNativeExpression;
        public static final boolean DEFAULT_EXPRESSION_VALUE = false;

        public SimpleColumn(String colName, String alias) {
            super(alias, Type.SIMPLE);
            this.colName = colName;
            this.isNativeExpression = DEFAULT_EXPRESSION_VALUE;
        }

        @JsonCreator
        public SimpleColumn(@JsonProperty("colName") String colName,
            @JsonProperty("alias") String alias,
            @JsonProperty("isNativeExpression") boolean isNativeExpression) {
            super(alias, Type.SIMPLE);
            this.colName = colName;
            this.isNativeExpression = isNativeExpression;
        }

        @Override
        public void accept(DSQueryVisitor visitor) {
            visitor.visit(this);
        }

        @Override
        public String getName() {return colName;}


        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SimpleColumn)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            SimpleColumn that = (SimpleColumn) o;
            return isNativeExpression == that.isNativeExpression &&
                Objects.equal(colName, that.colName);
        }
    }

    public static class Aggregation extends SelectColumn {

        public final AggregationType aggregationType;
        public final String colName;
        @Getter
        private final AggregationOptionsExpr options;
        public final boolean isNativeExpression;

        @JsonCreator
        public Aggregation(@JsonProperty("colName") String colName,
            @JsonProperty("aggregationType") AggregationType aggregationType,
            @JsonProperty("alias") String alias,
            @JsonProperty("options") AggregationOptionsExpr options,
            @JsonProperty("isNativeExpression") boolean isNativeExpression) {

            super(alias, Type.AGGREGATION);
            this.colName = colName;
            this.aggregationType = aggregationType;
            this.options = options;
            this.isNativeExpression = isNativeExpression;
        }

        @Override
        public void accept(DSQueryVisitor visitor) {
            try {
                visitor.visit(this, options.evaluate(visitor.getParamValues()) );
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getName() {
            return colName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Aggregation)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            Aggregation that = (Aggregation) o;
            return aggregationType == that.aggregationType &&
                Objects.equal(colName, that.colName) &&
                Objects.equal(getOptions(), that.getOptions()) &&
                isNativeExpression == that.isNativeExpression;
        }
    }

    public static class AggregationOptionsExpr implements  Evaluable<AggregationOptions>, Serializable
    {
        @Getter
        @Setter
        private Optional<EvalExp> fractileExp = Optional.absent();

        public AggregationOptionsExpr withFractile(EvalExp fractile)
        {
            this.fractileExp = Optional.of(fractile);
            return this;
        }


        public Optional<EvalExp> getFractile() {
            return fractileExp;
        }

        @Override public AggregationOptions evaluate(
                Map<String, String[]> paramValues) throws ExprEvalException {

            Optional<Double> fractileVal = Optional.absent();
            if(fractileExp.isPresent())
            {
                fractileVal = Optional.of( Double.valueOf(String.valueOf(
                        fractileExp.get().evaluate(paramValues))) );
                Preconditions.checkState(fractileVal.get()>0 && fractileVal.get()<=1,  "fractile value should be > 0 and <= 1");
            }

            return new AggregationOptions(fractileVal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AggregationOptionsExpr)) {
                return false;
            }
            AggregationOptionsExpr that = (AggregationOptionsExpr) o;
            return Objects.equal(getFractileExp(), that.getFractileExp());
        }
    }
    public static class AggregationOptions implements Serializable
    {
        public final Optional<Double> fractile ;

        public AggregationOptions(Optional<Double> fractile) {
            this.fractile = fractile;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof AggregationOptions)) {
                return false;
            }
            AggregationOptions that = (AggregationOptions) o;
            return Objects.equal(fractile, that.fractile);
        }
    }

    public static class ConditionalAggregation extends SelectColumn {

        public final AggregationType type;
        public final String colName;
        public final Criteria criteria;
        public final boolean isNativeExpression;

        @JsonCreator
        public ConditionalAggregation(@JsonProperty("colName") String colName,
            @JsonProperty("type") AggregationType type,
            @JsonProperty("alias") String alias,
            @JsonProperty("criteria") Criteria criteria,
            @JsonProperty("isNativeExpression") boolean isNativeExpression) {
            super(alias, Type.CONDITIONAL_AGGREGATION);
            this.colName = colName;
            this.type = type;
            this.criteria = criteria;
            this.isNativeExpression = isNativeExpression;
        }

        @Override
        public void accept(DSQueryVisitor visitor) {
            visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ConditionalAggregation)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ConditionalAggregation that = (ConditionalAggregation) o;
            return type == that.type &&
                Objects.equal(colName, that.colName) &&
                Objects.equal(criteria, that.criteria) &&
                isNativeExpression == that.isNativeExpression;
        }
    }

    public static enum SeriesType {INSTANTANEOUS, CUMULATIVE, GROWTH}

    public static enum DownSampleUnit {
        Minutes, Hours, Days, Week, CalendarWeek, Month,
        CalendarMonth, Default
        }

    public static class DateHistogram extends  SelectColumn {

        public final String columnName;
        public final EvalExp seriesType;
        @Getter
        private final EvalExp start, end, interval, downsampleUnit;

        @JsonCreator
        public DateHistogram(@JsonProperty("alias") String alias,
            @JsonProperty("seriesType") EvalExp seriesType,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("start") EvalExp start,
            @JsonProperty("end") EvalExp end,
            @JsonProperty("interval") EvalExp interval,
            @JsonProperty("downsampleUnit") EvalExp downsampleUnit ) {
            super(alias, Type.DATE_HISTOGRAM);
            this.columnName = columnName;
            this.start = start;
            this.end = end;
            this.interval = interval;
            this.seriesType = seriesType;
            this.downsampleUnit = downsampleUnit;
        }

        @Override
        public void accept(DSQueryVisitor visitor) {
            Map<String, String[]> params = visitor.getParamValues();
            visitor.visitDateHistogram(
                    getAlias(),
                    columnName,
                    getStart(params),
                    getEnd(params),
                    getInterval(params),
                    getDownsampleUnit(params)
            );
        }

        @JsonIgnore
        public Date getStart(Map<String, String[]> params) {
            try {
                return (Date) start.evaluate(params);
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @JsonIgnore
        public Date getEnd(Map<String, String[]> params) {
            try {
                return (Date) end.evaluate(params);
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @JsonIgnore
        public Long getInterval(Map<String, String[]> params) {
            try {
                return (Long) interval.evaluate(params);
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @JsonIgnore
        public SeriesType getSeriesType(Map<String, String[]> params) {
            try {
                return SeriesType.valueOf(String.valueOf(seriesType.evaluate(params)));
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @JsonIgnore
        public DownSampleUnit getDownsampleUnit(Map<String, String[]> params) {
            try {
                return DownSampleUnit.valueOf(String.valueOf(downsampleUnit.evaluate(params)));
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getName(){
            return columnName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DateHistogram)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            DateHistogram that = (DateHistogram) o;
            return Objects.equal(columnName, that.columnName) &&
                Objects.equal(seriesType, that.seriesType) &&
                Objects.equal(getStart(), that.getStart()) &&
                Objects.equal(getEnd(), that.getEnd()) &&
                Objects.equal(getInterval(), that.getInterval());
        }
    }

    public static class Histogram extends  SelectColumn {

        public final String columnName;
        @Getter
        private final EvalExp start, end, interval;

        @JsonCreator
        public Histogram(@JsonProperty("alias") String alias,
            @JsonProperty("type") Type type,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("start") EvalExp start,
            @JsonProperty("end") EvalExp end,
            @JsonProperty("interval") EvalExp interval) {
            super(alias, Type.HISTOGRAM);
            this.columnName = columnName;
            this.start = start;
            this.end = end;
            this.interval = interval;
        }

        @Override
        public void accept(DSQueryVisitor visitor) {
            Map<String, String[]> params = visitor.getParamValues();
            visitor.visitHistogram(
                    getAlias(),
                    columnName,
                    getStart(params),
                    getEnd(params),
                    getInterval(params)
            );
        }

        @JsonIgnore
        public long getStart(Map<String, String[]> params) {
            try {
                return Long.valueOf(String.valueOf(start.evaluate(params)));
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @JsonIgnore
        public long getEnd(Map<String, String[]> params) {
            try {
                return Long.valueOf(String.valueOf(end.evaluate(params)));
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @JsonIgnore
        public long getInterval(Map<String, String[]> params) {
            try {
                return Long.valueOf(String.valueOf(interval.evaluate(params)));
            } catch (ExprEvalException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Histogram)) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            Histogram histogram = (Histogram) o;
            return Objects.equal(columnName, histogram.columnName) &&
                Objects.equal(start, histogram.start) &&
                Objects.equal(end, histogram.end) &&
                Objects.equal(interval, histogram.interval);
        }
    }

//    public static class SelectColumnExp extends Exp {
//        public final SelectColumn selectColumn;
//
//        public SelectColumnExp(SelectColumn selectColumn) {
//            this.selectColumn = selectColumn;
//        }
//
//        public SelectColumnExp as(String alias){
//            this.selectColumn.alias = alias;
//            return this;
//        }
//
//        @Override
//        protected Set<Param> getParameters() {
//            return ImmutableSet.of();
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (!(o instanceof SelectColumnExp)) return false;
//            if (!super.equals(o)) return false;
//
//            SelectColumnExp that = (SelectColumnExp) o;
//
//            if (!selectColumn.equals(that.selectColumn)) return false;
//
//            return true;
//        }
//
//        @Override
//        public int hashCode() {
//            int result = super.hashCode();
//            result = 31 * result + selectColumn.hashCode();
//            return result;
//        }
//    }

    @JsonIgnore
    public String getName()
    {
        /**
         * By default return the alias
         */
        return alias;
    }

    @Override
    public String toString() {
        return "SelectColumn{" +
                "alias='" + alias + '\'' +
                '}';
    }
}