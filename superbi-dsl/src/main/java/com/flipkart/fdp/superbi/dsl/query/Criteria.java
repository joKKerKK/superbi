package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.base.Function;

/**
* User: shashwat
* Date: 02/01/14
 *
 * Denotes an criteria in a query.
*/

@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include= JsonTypeInfo.As.PROPERTY, property="@class")
public abstract class Criteria {

    public static class F {
        public static Function<Criteria, CriteriaVisitor> acceptor(CriteriaVisitor visitor) {
            return new AcceptorFunction(visitor);
        }

        static class AcceptorFunction implements Function<Criteria, CriteriaVisitor> {
            private final CriteriaVisitor visitor;

            AcceptorFunction(CriteriaVisitor visitor) {
                this.visitor = visitor;
            }

            @Override
            public CriteriaVisitor apply(Criteria input) {
                return input.accept(visitor);
            }
        }
    }

    //pre-order traversal
    public abstract CriteriaVisitor accept(CriteriaVisitor visitor);

    public abstract boolean equals(Object obj);
}
