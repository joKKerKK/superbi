package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.base.Objects;
import java.util.List;

/**
 * User: shashwat
 * Date: 02/01/14
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class LogicalOp extends Criteria {

    public enum Type {AND, OR, NOT}

    public abstract List<Criteria> getCriteria();

    @JsonIgnore
    public abstract Type getType();

    @Override
    public CriteriaVisitor accept(CriteriaVisitor visitor) {
        final CriteriaVisitor childVisitor = visitor.visit(this);

        for (Criteria criteria : getCriteria()) {
            criteria.accept(childVisitor);
        }
        return visitor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final LogicalOp other = getClass().cast(o);
        return Objects.equal(getCriteria(), other.getCriteria());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCriteria());
    }
}
