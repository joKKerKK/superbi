package com.flipkart.fdp.superbi.dsl.query.visitors.impl;

import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DateRangePredicate;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 * User: shashwat
 * Date: 17/01/14
 */
public class ParamVisitor extends DefaultDSQueryVisitor {
    private final Set<Param> params = Sets.newHashSet();
    private final CriteriaVisitor criteriaVisitor = new QueryCriteriaVisitor();

    public ParamVisitor(Map<String, String[]> paramValues) {
        super(paramValues);
    }

    @Override
    public void visit(DateRangePredicate dateRangePredicate) {
        dateRangePredicate.accept(criteriaVisitor);
    }

    @Override
    public void visit(Criteria criteria) {
        criteria.accept(criteriaVisitor);
    }

    public Set<Param> getParams() {
        return ImmutableSet.copyOf(params);
    }

    class QueryCriteriaVisitor extends DefaultCriteriaVisitor {

        @Override
        public CriteriaVisitor visit(Param param) {
            params.add(param);
            return this;
        }
    }
}
