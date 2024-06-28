package com.flipkart.fdp.superbi.dsl.query.visitors.impl;

import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;

/**
 * User: shashwat
 * Date: 17/01/14
 */
public class DefaultCriteriaVisitor implements CriteriaVisitor {
    @Override
    public CriteriaVisitor visit(Predicate predicate) {
        return this;
    }

    @Override
    public CriteriaVisitor visit(LogicalOp logicalOp) {
        return this;
    }

    @Override
    public CriteriaVisitor visit(Exp expression) {
        return this;
    }

    @Override
    public CriteriaVisitor visit(EvalExp expression) {
        return this;
    }

    @Override
    public CriteriaVisitor visit(Param param) {
        return this;
    }
}
