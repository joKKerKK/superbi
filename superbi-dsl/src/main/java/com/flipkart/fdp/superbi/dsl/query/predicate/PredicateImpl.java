package com.flipkart.fdp.superbi.dsl.query.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.ExprEvalException;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.ParamExp;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: aniruddha.gangopadhyay Date: 16/03/14 Time: 4:42 PM
 */
public class PredicateImpl extends Predicate {

  public final ColumnExp column;
  public final EvalExp type;
  public final List<Exp> values;

  @JsonCreator
  public PredicateImpl(@JsonProperty("column") ColumnExp column,
      @JsonProperty("type") EvalExp type, @JsonProperty("values") Exp... values) {
    this.column = column;
    this.type = type;
    this.values = Arrays.asList(values);
  }

  @Override
  protected List<Exp> getExpressions() {
    List<Exp> exps = Lists.newArrayList();
    exps.add(column);
    exps.addAll(values);
    return exps;
  }

  @Override
  public Type getType(Map<String, String[]> paramValues) {
    try {
      // TODO have to fix this, use generics
      return Type.valueOf(String.valueOf(type.evaluate(paramValues)));
    } catch (ExprEvalException e) {
      throw new RuntimeException("Invalid predicate type " + e);
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PredicateImpl)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    PredicateImpl predicate = (PredicateImpl) o;
    return Objects.equal(column, predicate.column) &&
        Objects.equal(type, predicate.type) &&
        Objects.equal(values, predicate.values);
  }
}
