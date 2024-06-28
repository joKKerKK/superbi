package com.flipkart.fdp.superbi.dsl.query.exp;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.base.Objects;
import java.io.Serializable;

/**
 * User: shashwat
 * Date: 16/01/14
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class EvalExp extends Exp implements Evaluable<Object>, Serializable {

    @JsonIgnore
    public abstract DataType getValueType();

    @Override
    protected CriteriaVisitor doVisitThis(CriteriaVisitor visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) return false;
        final EvalExp other = getClass().cast(o);
        return Objects.equal(getValueType(), other.getValueType());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), getValueType());
    }
}
