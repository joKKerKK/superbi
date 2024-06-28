package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;

/**
 * User: shashwat
 * Date: 02/01/14
 */
public class Param implements Serializable {
    public final String name;
    public final boolean isMultiple;
    public final DataType dataType;

    @JsonCreator
    public Param(@JsonProperty("name") String name,
        @JsonProperty("multiple") boolean multiple,
        @JsonProperty("dataType") DataType dataType) {
        this.name = name;
        isMultiple = multiple;
        this.dataType = dataType;
    }

    public Object getValue(Map<String, String[]> paramValues) {
        final String[] values = paramValues.get(name);
        if(ArrayUtils.isEmpty(values))
            throw new ParamMissingException(String.format("Param %s does not exist in provided values", name));

        return isMultiple ? values(values) : value(values[0]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Param other = getClass().cast(o);
        return Objects.equal(name, other.name) &&
                Objects.equal(isMultiple, other.isMultiple) &&
                Objects.equal(dataType, other.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, dataType, isMultiple);
    }

    public CriteriaVisitor accept(CriteriaVisitor visitor) {
        return visitor.visit(this);
    }

    private Object value(String value) {
        return dataType.toValue(value);
    }

    private List<Object> values(String[] values) {
        return FluentIterable.from(Arrays.asList(values)).transform(new Function<String, Object>() {
            @Override
            public Object apply(String input) {
                return value(input);
            }
        }).toList();
    }

    public static class F {
        public static final Function<Param, String> getName = new Function<Param, String>() {
            @Override
            public String apply(Param input) {
                return input.name;
            }
        };

        public static Function<Param, Map.Entry<String, Object>> evalParamFunc(Map<String, String[]> paramValues) {
            return new EvalFunction(paramValues);
        }

        public static class EvalFunction implements Function<Param, Map.Entry<String, Object>> {
            private final Map<String, String[]> paramValues;

            public EvalFunction(Map<String, String[]> paramValues) {
                this.paramValues = paramValues;
            }

            @Override
            public Map.Entry<String, Object> apply(final Param param) {
                return Maps.immutableEntry(param.name, param.getValue(paramValues));
            }
        }

    }
}
