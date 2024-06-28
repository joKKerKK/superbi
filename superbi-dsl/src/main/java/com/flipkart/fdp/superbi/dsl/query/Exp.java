package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import java.util.Set;

/**
 * User: shashwat
 * Date: 02/01/14
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class Exp {
    @JsonIgnore
    protected abstract Set<Param> getParameters();

    // pre-order traversal
    public CriteriaVisitor accept(CriteriaVisitor visitor) {
        final CriteriaVisitor childVisitor = doVisitThis(visitor);
        for (Param param : getParameters()) {
            param.accept(childVisitor);
        }

        return visitor;
    }

    // template method pattern - method can be replaced by the derived class completely
    protected CriteriaVisitor doVisitThis(CriteriaVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Exp other = getClass().cast(o);
        return Objects.equal(getParameters(), other.getParameters());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getParameters());
    }

    public static class F {
        public static final Function<Exp, Set<Param>> getParameters = new Function<Exp, Set<Param>>() {
            @Override
            public Set<Param> apply(Exp input) {
                return input.getParameters();
            }
        };
    }
}
