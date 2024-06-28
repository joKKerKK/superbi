package com.flipkart.fdp.superbi.cosmos.data.query.result;

import com.flipkart.fdp.superbi.cosmos.data.query.QuerySubmitResult;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 01/07/14
 */

public abstract class StreamingQueryResult extends QuerySubmitResult implements Iterable<List<Object>> {
    @Override
    public abstract Iterator<List<Object>> iterator();
    public abstract Schema getSchema();
    public abstract void close();
}
