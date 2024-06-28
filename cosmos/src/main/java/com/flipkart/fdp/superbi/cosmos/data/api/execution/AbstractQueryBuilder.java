package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultDSQueryVisitor;
import com.google.common.base.Optional;
import java.util.Map;

/**
 * Created by amruth.s on 01-11-2014.
 */

public abstract class AbstractQueryBuilder extends DefaultDSQueryVisitor {

    public final DSQuery query;
    public final AbstractDSLConfig config;

    Optional<Object> builtQueryOptional = Optional.absent();

    public AbstractQueryBuilder(DSQuery query, Map<String, String[]> paramValues, AbstractDSLConfig config) {
        super(paramValues);
        this.query = query;
        this.config = config;
    }
    @LogExecTime
    public Object buildQuery() {
        if(!builtQueryOptional.isPresent()) {
            query.accept(this);
            builtQueryOptional = Optional.of(buildQueryImpl());
        }
        return builtQueryOptional.get();
    }

    protected abstract Object buildQueryImpl();
}
