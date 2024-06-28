package com.flipkart.fdp.superbi.dsl.query.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import java.util.Map;

/**
 * User: aniruddha.gangopadhyay
 * Date: 25/04/14
 * Time: 11:28 AM
 */
public class LessThanEqualPredicate extends BinaryPredicate<ColumnExp>{
    @JsonCreator
    public LessThanEqualPredicate(@JsonProperty("left") ColumnExp left,
        @JsonProperty("right") Exp right) {
        super(left, right);
    }

    @Override
    public Type getType(Map<String, String[]> paramValues) {
        return Type.lte;
    }
}
