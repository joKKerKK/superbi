package com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.dao.DBIFactory;
import com.flipkart.fdp.superbi.dsl.query.*;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.tomcat.dbcp.dbcp.ConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.DriverManagerConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolableConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;
import org.apache.tomcat.dbcp.pool.impl.GenericObjectPool;
import org.skife.jdbi.v2.DBI;

/**
 * Created by amruth.s on 28/09/14.
 */

public class MysqlExecutor extends DSQueryExecutor {

    private final MysqlDao dao;

    public MysqlExecutor(String sourceName, String sourceType, String jdbcUrl, String username,
                         String password, Map<String, String> otherAttributes) {
        super(sourceName, sourceType, new MysqlDSLConfig(otherAttributes));
        GenericObjectPool connectionPool = new GenericObjectPool();
        connectionPool.setMaxActive(3);
        final ConnectionFactory cf = new DriverManagerConnectionFactory(
                jdbcUrl,
                username,
                password);
        new PoolableConnectionFactory(cf, connectionPool, null, "SELECT 1" , false, false);
        final PoolingDataSource dataSource = new PoolingDataSource(connectionPool);
        final DBIFactory factory = new DBIFactory();
        final DBI dbi = factory.build(dataSource);
        dao = dbi.onDemand(MysqlDao.class);
    }

    @Override
    public AbstractQueryBuilder getTranslator(DSQuery query, Map<String, String[]> paramValues) {
        return new MysqlQueryBuilder(query, paramValues, (MysqlDSLConfig) config);
    }

    @Override
    public Object explainNative(Object nativeQuery) {
        return "TBD";
    }

    @Override
    public QueryResult executeNative(Object nativeQuery, ExecutionContext context) {
        final List<ResultRow> rows = dao.execute(String.valueOf(nativeQuery));
        return new QueryResult(
                null,
                rows);
    }

    @Override
    public QueryResult executeNative(Object object, ExecutionContext context, ICacheClient<String, QueryResult> cacheClient) {
        //currently no caching for mysql
        return  executeNative( object,  context);
    }


    @Override
    public StreamingQueryResult executeStreamNative(Object object, ExecutionContext context) {
        final QueryResult result = executeNative(object, context);
        final Iterator<ResultRow> iterator = result.data.iterator();

        return new StreamingQueryResult() {

            @Override
            public Iterator<List<Object>> iterator() {
                return new Iterator<List<Object>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public List<Object> next() {
                        return iterator.next().row;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public Schema getSchema() {
                return null;
            }

            @Override
            public void close() {

            }
        };
    }
}
