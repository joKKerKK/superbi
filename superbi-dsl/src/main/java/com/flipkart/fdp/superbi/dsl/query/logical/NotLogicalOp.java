package com.flipkart.fdp.superbi.dsl.query.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Criteria;

/**
* User: shashwat
* Date: 16/01/14
*/
public class NotLogicalOp extends BaseLogicalOp {

    @JsonCreator
    public NotLogicalOp(@JsonProperty("criteria") Criteria criteria) {
        super(criteria);
    }

    @Override
    public Type getType() {
        return Type.NOT;
    }
}
