package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.base.Objects;
import java.util.List;
import java.util.Map;

/**
* User: shashwat
* Date: 02/01/14
*/
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class Predicate extends Criteria {
    @JsonIgnore
    protected abstract List<Exp> getExpressions(); //ordered list of expressions

    public CriteriaVisitor accept(CriteriaVisitor visitor) {
        final CriteriaVisitor childVisitor = visitor.visit(this);

        for (Exp exp : getExpressions()) {
            exp.accept(childVisitor);
        }
        return visitor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Predicate other = getClass().cast(o);
        return Objects.equal(getExpressions(), other.getExpressions());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getExpressions());
    }

    @JsonIgnore
    public List<Exp> getExp() {
        return getExpressions();
    }

    @JsonIgnore
    public abstract Type getType(Map<String, String[]> paramValues);

    public enum Type {
        lt, gt, lte, gte, date_range, in, not_in, eq, neq, like, is_null, is_not_null, native_filter
    }
}
