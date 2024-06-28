package com.flipkart.fdp.superbi.dsl.query.factory;

import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.Exp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.flipkart.fdp.superbi.dsl.query.exp.ParamExp;
import com.flipkart.fdp.superbi.dsl.query.logical.AndLogicalOp;
import com.flipkart.fdp.superbi.dsl.query.logical.NotLogicalOp;
import com.flipkart.fdp.superbi.dsl.query.logical.OrLogicalOp;
import com.flipkart.fdp.superbi.dsl.query.predicate.EqualsPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.GreaterThanEqualPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.GreaterThanPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.InPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.LessThanEqualPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.LessThanPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.LikePredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.NotEqualsPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.PredicateImpl;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * User: shashwat
 * Date: 24/01/14
 */
@HasFactoryMethod
public class CriteriaFactory {
    private CriteriaFactory() {}

    public static OrLogicalOp OR(Criteria criteria1, Criteria criteria2, Criteria... criteria) {
        final List<Criteria> criteriaList =
                ImmutableList.<Criteria>builder().add(criteria1).add(criteria2).add(criteria).build();
        return new OrLogicalOp(criteriaList);
    }

    public static AndLogicalOp AND(Criteria criteria1, Criteria criteria2, Criteria... criteria) {
        final List<Criteria> criteriaList =
                ImmutableList.<Criteria>builder().add(criteria1).add(criteria2).add(criteria).build();
        return new AndLogicalOp(criteriaList);
    }

    public static AndLogicalOp AND(List<Criteria> criteria) {
        final List<Criteria> criteriaList =
                ImmutableList.copyOf(criteria);
        return new AndLogicalOp(criteriaList);
    }



    private static NotLogicalOp NOT(Criteria c) {
        return new NotLogicalOp(c);
    }

    public static EqualsPredicate EQ(ColumnExp lExp, Exp rExp) {
        return new EqualsPredicate(lExp, rExp);
    }

    public static InPredicate IN(ColumnExp columnExp, List<Exp> exps){
        return new InPredicate(columnExp,exps);
    }

    public static PredicateImpl PRED(ColumnExp columnExp, Predicate.Type type, Exp... exps){
        LiteralEvalExp exp = new LiteralEvalExp(String.valueOf(type));
        return new PredicateImpl(columnExp,exp, exps);
    }

    public static PredicateImpl PRED(ColumnExp columnExp, ParamExp type, Exp... exps){
        return new PredicateImpl(columnExp, type, exps);
    }

    public static InPredicate IN(ColumnExp columnExp, Exp... exps){
        return new InPredicate(columnExp, exps);
    }

    public static LikePredicate LIKE(ColumnExp columnExp, Exp exp){
        return new LikePredicate(columnExp,exp);
    }

    public static GreaterThanPredicate GT(ColumnExp columnExp, Exp exp){
        return new GreaterThanPredicate(columnExp,exp);
    }

    public static LessThanPredicate LT(ColumnExp columnExp, Exp exp){
        return new LessThanPredicate(columnExp,exp);
    }

    public static NotEqualsPredicate NEQ(ColumnExp columnExp, Exp exp){
        return new NotEqualsPredicate(columnExp,exp);
    }

    public static GreaterThanEqualPredicate GTE(ColumnExp columnExp, Exp exp){
        return new GreaterThanEqualPredicate(columnExp,exp);
    }

    public static LessThanEqualPredicate LTE(ColumnExp columnExp, Exp exp){
        return new LessThanEqualPredicate(columnExp,exp);
    }
}

