package com.flipkart.fdp.superbi.dsl.query.logical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import java.util.List;

/**
 * User: shashwat Date: 16/01/14
 */
public class AndLogicalOp extends BaseLogicalOp {

  @JsonCreator
  public AndLogicalOp(@JsonProperty("criteria") List<Criteria> criteria) {
    super(criteria);
  }

  @Override
  public LogicalOp.Type getType() {
    return Type.AND;
  }

}
