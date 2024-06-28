package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

/**
* User: shashwat
* Date: 16/01/14
*/
public class LiteralEvalExp extends EvalExp {

    public static final LiteralEvalExp NONE = new LiteralEvalExp(null){
        @Override public String toString() {
            return "LiteralEvalExp{NONE}";
        }
    };
    public final Object value;
    @Getter
    public final DataType valueType;

    public LiteralEvalExp(Object value) {
        this.value = value;
        this.valueType = DataType.from(value);
    }

    @JsonCreator
    public LiteralEvalExp(@JsonProperty("value") Object value, @JsonProperty("valueType") DataType valueType) {
        this.valueType = valueType;
        if(valueType != DataType.NULL)
          this.value = valueType.toValue(String.valueOf(value));
        else
          this.value = null;
    }

    @Override
    protected Set<Param> getParameters() {
        return ImmutableSet.of();
    }

    @Override
    public Object evaluate(Map<String, String[]> paramValues) {
        return value;
    }

//    @Override
//    public DataType getValueType() {
//        return DataType.
//            from(value);
//    }

    @Override
    public String toString() {
        return "LiteralEvalExp{" +
                "value=" + value +
                ", type=" + getValueType() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LiteralEvalExp)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LiteralEvalExp that = (LiteralEvalExp) o;
        return Objects.equal(value, that.value) &&
            getValueType() == that.getValueType();
    }

    public static class F {
        public static Function<Object, EvalExp> mkLiteral = new Function<Object, EvalExp>() {
            @Override
            public LiteralEvalExp apply(Object input) {
                return new LiteralEvalExp(input);
            }
        };
    }
}
