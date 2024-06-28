package com.flipkart.fdp.superbi.dsl.query.visitors;

import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;

/**
 * User: shashwat
 * Date: 16/01/14
 */
public interface CriteriaVisitor {
    CriteriaVisitor visit(Predicate predicate);

    CriteriaVisitor visit(LogicalOp logicalOp);

    CriteriaVisitor visit(Exp expression);

    CriteriaVisitor visit(EvalExp expression);

    CriteriaVisitor visit(Param param);
}
