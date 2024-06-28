package com.flipkart.fdp.superbi.dsl.query.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import java.util.Map;

/**
* User: shashwat
* Date: 16/01/14
*/
public class EqualsPredicate extends BinaryPredicate<ColumnExp> {
    @JsonCreator
    public EqualsPredicate(@JsonProperty("column") ColumnExp column,@JsonProperty("exp") Exp exp) {
        super(column, exp);
    }

    @Override
    public Type getType(Map<String, String[]> paramValues) {
        return Type.eq;
    }
}
