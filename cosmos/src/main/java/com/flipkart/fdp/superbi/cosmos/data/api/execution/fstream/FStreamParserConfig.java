package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import java.util.Map;
import java.util.Set;

public class FStreamParserConfig extends AbstractDSLConfig {
  @Override
  public Set<LogicalOp.Type> getSupportedLogicalOperators() {
    return null;
  }

  @Override
  public Set<Predicate.Type> getSupportedPredicates() {
    return null;
  }

  @Override
  public Set<AggregationType> getSupportedAggregations() {
    return null;
  }

  public FStreamParserConfig(Map<String, String> overrides) {
    super(overrides);
  }
}
