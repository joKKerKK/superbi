package com.flipkart.fdp.superbi.cosmos.cache.dataSource;

/**
 * Created by piyush.mukati on 13/05/15.
 */

import java.util.Optional;

/**
 * contract for data source
 * @param <Q> Query type
 * @param <R> Result Type
 */
public interface IDataSource<Q, R> {
    /**
     *
     * @param q query to be executed against the data source
     * @return result of query execution
     */
    Optional<R> execute(Q q);
}
