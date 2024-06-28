package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;

/**
* User: shashwat
* Date: 16/01/14
*/
public class GenericEvalExp extends EvalExp {
    public final String expression;
    public final ExpEvaluator evaluator;
    private final DataType valueType;
    private final Set<Param> params;

    @JsonCreator
    public GenericEvalExp(@JsonProperty("expression") String expression,
        @JsonProperty("valueType") DataType valueType,
        @JsonProperty("params") Set<Param> params,
        @JsonProperty("evaluator") ExpEvaluator evaluator) {
        this.expression = expression;
        this.valueType = valueType;
        this.params = params;
        this.evaluator = evaluator;
    }

    @Override
    protected Set<Param> getParameters() {
        return params;
    }

    @Override
    public DataType getValueType() {
        return valueType;
    }

    @Override
    public Object evaluate(Map<String, String[]> paramValues) {
        return evaluator.eval(expression, params, Maps.filterKeys(paramValues, Predicates.in(paramNames())));
    }

    private Set<String> paramNames() {
        return FluentIterable.from(params).transform(Param.F.getName).toSet();
    }


    @Override
    public String toString() {
        return "GenericEvalExp{" +
                "expression='" + expression + '\'' +
                ", valueType=" + valueType +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;

        final GenericEvalExp other = getClass().cast(o);

        return Objects.equal(expression, other.expression) &&
                Objects.equal(evaluator, other.evaluator);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), expression, evaluator);
    }
}
