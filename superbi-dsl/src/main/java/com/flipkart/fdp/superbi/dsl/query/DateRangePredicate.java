package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.Evaluable;
import com.flipkart.fdp.superbi.dsl.query.exp.ExprEvalException;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * User: shashwat
 * Date: 16/01/14
 */
public class DateRangePredicate extends Predicate implements Evaluable<DateRange> {
    public ColumnExp columnExp;
    private Optional<ColumnExp> timeColumnExp;
    public EvalExp start, end;
    public Optional<EvalExp> interval;

    // expressions should evaluate to date objects
    public DateRangePredicate(ColumnExp column,EvalExp start, EvalExp end) {
        this(column, Optional.<ColumnExp>absent(),start, end, Optional.<EvalExp>absent());
    }

    public DateRangePredicate(ColumnExp column,ColumnExp timeColumnExp,EvalExp start, EvalExp end) {
        this(column, Optional.of(timeColumnExp),start, end, Optional.<EvalExp>absent());
    }

    public DateRangePredicate(ColumnExp column,ColumnExp timeColumnExp,EvalExp start, EvalExp end,EvalExp interval) {
        this(column, Optional.of(timeColumnExp),start, end, Optional.of(interval));
    }

    // expressions should evaluate to date objects
    public DateRangePredicate(ColumnExp column, EvalExp start, EvalExp end, EvalExp interval) {
        this(column, Optional.<ColumnExp>absent(),start, end, Optional.of(interval));
    }

    @JsonCreator
    protected DateRangePredicate(@JsonProperty("columnExp") ColumnExp columnExp,
        @JsonProperty("timeColumnExp") Optional<ColumnExp> timeColumnExp,
        @JsonProperty("start") EvalExp start,
        @JsonProperty("end") EvalExp end,
        @JsonProperty("interval") Optional<EvalExp> interval) {
        this.columnExp = columnExp;
        this.timeColumnExp = timeColumnExp;
        this.start = start;
        this.end = end;
        this.interval = interval;
    }

    public DateRangePredicate(DateRange dateRange) {
        this(new ColumnExp(dateRange.getColumn()),
                F.getOptionalColumnExp.apply(dateRange.getTimeColumn()),
                new LiteralEvalExp(dateRange.getStart()), new LiteralEvalExp(dateRange.getEnd()),
                dateRange.getIntervalMs().transform(LiteralEvalExp.F.mkLiteral));
    }


    @Override
    protected List<Exp> getExpressions() {
        final ArrayList<Exp> expList = Lists.<Exp>newArrayList(columnExp, start, end);
        if (interval.isPresent()) {
            expList.add(interval.get());
        }
        return expList;
    }

    @Override
    public Type getType(Map<String, String[]> paramValues) {
        return Type.date_range;
    }

    // It is assumed that the result of the expression will be long; throw an exception otherwise
    @Override
    public DateRange evaluate(Map<String, String[]> params) throws ExprEvalException {
        final Date startDate = evalDateExpr(start, params);
        final Date endDate = evalDateExpr(end, params);
        final Evaluable.F.EvalFn<Object, EvalExp> evalFunc = Evaluable.F.evalFunc(params);
        final Optional<Long> intervalVal = interval.transform(Functions.compose(F.toLongFn, evalFunc));
        return DateRange.builder().column(columnExp.evaluateAndGetColName(params)).start(startDate).end(endDate).interval(intervalVal).build();
    }

    private Date evalDateExpr(EvalExp exp, Map<String, String[]> params) throws ExprEvalException {
        final Object result = exp.evaluate(params);
        final DataType valueType = exp.getValueType();

        if (DataType.isNumber(valueType)) {
            return new Date(((Number) result).longValue());
        }
        if (DataType.isDate(valueType)) {
            return (Date)result;
        }
        if (DataType.isString(valueType)) {
            return (Date)(DataType.DATETIME.toValue((String)result));
        }

        throw new ExprEvalException("Exp does not evaluate to a long or java.util.Date value: " + exp + " return value: " + result);
    }

    public static class F {

        private static Function<Object, Long> toLongFn = new Function<Object, Long>() {
            @Override
            public Long apply(Object input) {
                return (Long)input;
            }
        };


        private static final Function<Optional<String>,Optional<ColumnExp>> getOptionalColumnExp = new Function<Optional<String>, Optional<ColumnExp>>() {
            @Nullable
            @Override
            public Optional<ColumnExp> apply(@Nullable Optional<String> stringOptional) {
                if(stringOptional.isPresent()) {
                    return Optional.of(new ColumnExp(stringOptional.get()));
                }
                return Optional.<ColumnExp>absent();
            }
        };


    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DateRangePredicate)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DateRangePredicate that = (DateRangePredicate) o;
        return Objects.equal(columnExp, that.columnExp) &&
            Objects.equal(timeColumnExp, that.timeColumnExp) &&
            Objects.equal(start, that.start) &&
            Objects.equal(end, that.end) &&
            Objects.equal(interval, that.interval);
    }
}
