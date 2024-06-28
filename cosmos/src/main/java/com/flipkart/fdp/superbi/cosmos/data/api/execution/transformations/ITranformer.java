package com.flipkart.fdp.superbi.cosmos.data.api.execution.transformations;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import java.util.Map;

/**
 * Created by amruth.s on 17/12/14.
 */
public interface ITranformer {
    QueryResult apply(DSQuery query, Map<String, String[]> params, QueryResult result);
}
