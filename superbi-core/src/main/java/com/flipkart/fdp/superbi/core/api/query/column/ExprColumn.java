package com.flipkart.fdp.superbi.core.api.query.column;

import com.google.common.collect.Maps;
import java.util.Map;

/**
 * Created by rajesh.kannan on 12/01/15.
 */
public class ExprColumn  extends FactColumn {

    private String expr;
    private String alias;
    ExprColumn(String factName, String expr, String alias)
    {
        super(factName);
        this.expr = expr;
        this.alias = alias;
    }


    @Override
    public Map<String,Object> getMeta() {
        return Maps.newHashMap();
    }

    @Override
    public String getFQName() {
        return  alias;
    }

    @Override
    public String getRName() {
        return alias;
    }
}
