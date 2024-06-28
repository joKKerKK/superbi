package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;

/**
* User: shashwat
* Date: 16/01/14
*/
public class ParamExp extends EvalExp {
    public final Param param;

    @JsonCreator
    public ParamExp(@JsonProperty("param") Param param) {
        this.param = param;
    }

    @Override
    protected Set<Param> getParameters() {
        return ImmutableSet.of(param);
    }

    @Override
    public Object evaluate(Map<String, String[]> paramValues) {
        return param.getValue(paramValues);
    }

    @Override
    public DataType getValueType() {
        return param.dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ParamExp)) {
            return false;
        }
        ParamExp paramExp = (ParamExp) o;
        return Objects.equal(param, paramExp.param);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), param);
    }
}
