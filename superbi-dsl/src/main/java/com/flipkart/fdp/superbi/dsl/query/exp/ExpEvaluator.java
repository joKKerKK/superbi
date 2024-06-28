package com.flipkart.fdp.superbi.dsl.query.exp;


import com.flipkart.fdp.superbi.dsl.query.Param;
import java.util.Collection;
import java.util.Map;

/**
 * User: shashwat
 * Date: 03/01/14
 */
public interface ExpEvaluator {
    Object eval(String expression, Collection<Param> params, Map<String, String[]> paramValues);
}
