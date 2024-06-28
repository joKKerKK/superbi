package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;

/**
* User: shashwat
* Date: 16/01/14
*/

public class ColumnExp extends Exp {
    public final EvalExp columnName;

    public ColumnExp(String columnName) {
        this.columnName = new LiteralEvalExp(columnName);
    }

    @JsonCreator
    public ColumnExp(@JsonProperty("columnName") EvalExp columnName) {
        this.columnName = columnName;
    }

    @Override
    protected Set<Param> getParameters() {
        return ImmutableSet.of();
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;

        final ColumnExp other = getClass().cast(o);
        return Objects.equal(columnName, other.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), columnName);
    }

    public String evaluateAndGetColName(Map<String, String[]> paramValues) {
        try {
            return (String) columnName.evaluate(paramValues);
        } catch (ExprEvalException e) {
            throw new RuntimeException("Parser error - " + e);
        }
    }

    /**
     * This is provided for backward compatibility.
     * Will be removed soon
     *
     * @deprecated
     */
    @Deprecated
    public static Function<String, ColumnExp> fromColName = new Function<String, ColumnExp>() {
        @Override
        public ColumnExp apply(String input) {
            return new ColumnExp(input);
        }
    };

    public static Function<ColumnExp, String> getTransformer(final Map<String, String[]> params) {
        return new Function<ColumnExp, String>() {
            @Override
            public String apply(ColumnExp input) {
                return input.evaluateAndGetColName(params);
            }
        };
    }

    @Override
    public String toString() {
        return "ColumnExp{" +
                "columnName=" + columnName +
                '}';
    }
}
