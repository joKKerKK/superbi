package com.flipkart.fdp.superbi.dsl.query.factory;


import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;

/**
 * User: shashwat
 * Date: 29/01/14
 */
@HasFactoryMethod
public class DateRangeFactory {
    private DateRangeFactory() {}

    public static DateRangePredicate DATE_RANGE(ColumnExp column,EvalExp start, EvalExp end) {
        return new DateRangePredicate(column, start, end);
    }

    public static DateRangePredicate DATE_RANGE(ColumnExp column, EvalExp start, EvalExp end, EvalExp intervalMs) {
        return new DateRangePredicate(column, start, end, intervalMs);
    }

    public static DateRangePredicate DATE_RANGE(ColumnExp column,ColumnExp timeColumn, EvalExp start, EvalExp end, EvalExp intervalMs) {
        return new DateRangePredicate(column, timeColumn,start, end, intervalMs);
    }

    public static DateRangePredicate DATE_RANGE(ColumnExp column,ColumnExp timeColumn, EvalExp start, EvalExp end) {
        return new DateRangePredicate(column, timeColumn,start, end);
    }
}
