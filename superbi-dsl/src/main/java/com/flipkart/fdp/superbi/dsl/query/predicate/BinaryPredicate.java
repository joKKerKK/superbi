package com.flipkart.fdp.superbi.dsl.query.predicate;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
* User: shashwat
* Date: 16/01/14
*/
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class BinaryPredicate<LT extends Exp> extends Predicate {
    public final LT left;
    public final Exp right;

    public BinaryPredicate(LT left, Exp right) {
        this.left = left;
        this.right = right;
    }

    @Override
    protected List<Exp> getExpressions() {
        return ImmutableList.of(left, right);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinaryPredicate)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BinaryPredicate<?> that = (BinaryPredicate<?>) o;
        return Objects.equal(left, that.left) &&
            Objects.equal(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), left, right);
    }
}
