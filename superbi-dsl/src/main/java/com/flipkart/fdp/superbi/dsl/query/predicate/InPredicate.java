package com.flipkart.fdp.superbi.dsl.query.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: aniruddha.gangopadhyay
 * Date: 16/03/14
 * Time: 4:42 PM
 */
public class InPredicate extends Predicate {
    public final ColumnExp column;
    public final List<Exp> values;

    @JsonCreator
    public InPredicate(@JsonProperty("column") ColumnExp column,
        @JsonProperty("values") List<Exp> values){
        this.column =column;
        this.values = values;
    }

    public InPredicate(ColumnExp column, Exp... values){
        this.column = column;
        this.values = Arrays.asList(values);
    }

    @Override
    protected List<Exp> getExpressions() {
        List<Exp> exps = Lists.newArrayList();
        exps.add(column);
        exps.addAll(values);
        return exps;
    }

    @Override
    public Type getType(Map<String, String[]> paramValues) {
        return Type.in;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InPredicate)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        InPredicate that = (InPredicate) o;
        return Objects.equal(column, that.column) &&
            Objects.equal(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), column, values);
    }
}
