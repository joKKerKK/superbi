package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;

/**
 * Created by amruth.s on 13/12/14.
 */
public class OrderByExp {

    public enum Type {ASC, DESC}

    public static Function<? super String, ? extends OrderByExp> fromColName = new Function<String, OrderByExp>() {
        @Nullable
        @Override
        public OrderByExp apply(@Nullable String input) {
            return new OrderByExp(
                    new ColumnExp(input),
                    new LiteralEvalExp("ASC")
            );
        }
    };
    public final ColumnExp exp;
    public final EvalExp type;

    @JsonCreator
    public OrderByExp(@JsonProperty("exp") ColumnExp exp,
        @JsonProperty("type") EvalExp type) {
        this.exp = exp;
        this.type = type;
    }

    public String evalueateAndGetType(Map<String, String[]> paramValues) {
        try {
            return String.valueOf(type.evaluate(paramValues));
        } catch (ExprEvalException e) {
            throw new RuntimeException("Parser error - " + e);
        }
    }

    public static class OrderedBy implements Serializable{
        public final String colName;
        public final String type;

        @JsonCreator
        @Builder
        public OrderedBy(@JsonProperty("colName") String colName, @JsonProperty("type")String type) {
            this.colName = colName;
            this.type = type;
        }
    }

    /**
     * The transformer will return null if there was an illegal argument exception/ params not provided
     * @return
     */
    public static Function<? super OrderByExp, OrderedBy> getTransformer(final Map<String, String[]> params) {
        return new Function<OrderByExp, OrderedBy>() {
            @Override
            public OrderedBy apply(OrderByExp input) {
                try {
                    return new OrderedBy(input.exp.evaluateAndGetColName(params),
                            input.evalueateAndGetType(params));
                } catch (IllegalArgumentException e) {return null;}
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OrderByExp)) {
            return false;
        }
        OrderByExp that = (OrderByExp) o;
        return Objects.equal(exp, that.exp) &&
            Objects.equal(type, that.type);
    }
}

