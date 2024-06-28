package com.flipkart.fdp.superbi.refresher.dao.jdbc;

import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;

public class JdbcDataSourceDao implements DataSourceDao{

    protected final JdbcClient jdbcClient;

    public JdbcDataSourceDao(JdbcClient jdbcClient){
        this.jdbcClient = jdbcClient;
    }

    @Override
    public QueryResult getStream(DataSourceQuery dataSourceQuery) {
        return jdbcClient.getStreamingIterable(String.valueOf(dataSourceQuery.getNativeQuery()));
    }
}
