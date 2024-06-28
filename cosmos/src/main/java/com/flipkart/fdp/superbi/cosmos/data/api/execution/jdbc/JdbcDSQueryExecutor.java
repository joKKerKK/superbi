package com.flipkart.fdp.superbi.cosmos.data.api.execution.jdbc;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.tomcat.dbcp.dbcp.ConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.DriverManagerConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolableConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;
import org.apache.tomcat.dbcp.pool.impl.GenericObjectPool;

/**
 * Created by rajesh.kannan on 07-06-2015.
 */
public abstract class JdbcDSQueryExecutor extends DSQueryExecutor {

    protected JdbcDao dao;

    public JdbcDSQueryExecutor(String name, String sourceType, String jdbcUrl, String username,
            String password, AbstractDSLConfig config) {
        super(name, sourceType, config);

        final ConnectionFactory cf = new DriverManagerConnectionFactory(
                jdbcUrl,
                username,
                password);

        JdbcConnectionPoolCreator (cf, config);
    }

    public JdbcDSQueryExecutor(String name, String sourceType, String jdbcUrl, Properties properties,
                               AbstractDSLConfig config) {
        super(name, sourceType, config);

        final ConnectionFactory cf = new DriverManagerConnectionFactory(
                jdbcUrl,
                properties);

        JdbcConnectionPoolCreator (cf, config);
    }

    public void JdbcConnectionPoolCreator(ConnectionFactory cf, AbstractDSLConfig config) {

        GenericObjectPool connectionPool = new GenericObjectPool();
        connectionPool.setTestOnBorrow(false);
        connectionPool.setTestOnReturn(false);
        connectionPool.setTestWhileIdle(true);
        connectionPool.setTimeBetweenEvictionRunsMillis(60 * 1000L);
        connectionPool.setMaxActive(config.getMaxActiveConnections());


        new PoolableConnectionFactory(cf, connectionPool, null, "SELECT 1" , false, false);
        PoolingDataSource dataSource = new PoolingDataSource(connectionPool);

        dao = new JdbcDao(dataSource, (int) (config.getQueryTimeOutMs()/1000),(List<String>)config.getInitScripts());
    }

    public  abstract AbstractQueryBuilder getTranslator(DSQuery query, Map<String, String[]> paramValues) ;


    @Override
    public Object explainNative(Object nativeQuery) {
        return dao.explain(nativeQuery);
    }

    @Override
    public final QueryResult executeNative(Object object, ExecutionContext context) {
        return dao.execute(String.valueOf(object));
    }

    @Override
    public final StreamingQueryResult executeStreamNative(Object object, ExecutionContext context) {
        return dao.getStreamingIterable(String.valueOf(object));
    }
}
