package com.flipkart.fdp.superbi.dsl.query.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.google.common.base.Objects;
import java.util.Arrays;
import java.util.List;

/**
* User: shashwat
* Date: 16/01/14
*/
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class BaseLogicalOp extends LogicalOp {

    private final List<Criteria> criteria;

    @JsonCreator
    public BaseLogicalOp(@JsonProperty("criteria") List<Criteria> criteria) {
        this.criteria = criteria;
    }

    public BaseLogicalOp(Criteria... criteria) {
        this(Arrays.asList(criteria));
    }

    @Override
    public List<Criteria> getCriteria() {
        return criteria;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseLogicalOp)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BaseLogicalOp that = (BaseLogicalOp) o;
        return Objects.equal(getCriteria(), that.getCriteria());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), getCriteria());
    }
}
