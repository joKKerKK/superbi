package com.flipkart.fdp.superbi.cosmos.data.query;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import java.util.Optional;

/**
 * Created by chandrasekhar.v on 25/11/15.
 */
public enum QueryRequestUtils {
    instance;

    public static Optional<QueryResultMeta> getQueryMeta (ICacheClient<String, QueryResultMeta> queryResultMetaStore, String handle) {
        return queryResultMetaStore.get(handle);
    }

    public static Optional<QueryResult> getQueryResults (ICacheClient<String, QueryResult> queryResultStore, String handle) {
        return queryResultStore.get(handle);
    }
}
