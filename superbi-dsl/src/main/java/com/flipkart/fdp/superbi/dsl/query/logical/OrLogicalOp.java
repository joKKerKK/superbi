package com.flipkart.fdp.superbi.dsl.query.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import java.util.List;

/**
* User: shashwat
* Date: 16/01/14
*/
public class OrLogicalOp extends BaseLogicalOp {
    @JsonCreator
    public OrLogicalOp(@JsonProperty("criteria") List<Criteria> criteria) {
        super(criteria);
    }

    @Override
    public Type getType() {
        return Type.OR;
    }
}
