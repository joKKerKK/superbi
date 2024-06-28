package com.flipkart.fdp.superbi.refresher.dao;

import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;

public interface DataSourceDao {
    QueryResult getStream(DataSourceQuery nativeQuery);
}
