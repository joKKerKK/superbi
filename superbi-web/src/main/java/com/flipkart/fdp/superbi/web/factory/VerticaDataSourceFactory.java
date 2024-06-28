package com.flipkart.fdp.superbi.web.factory;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig;
import com.google.common.base.Preconditions;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.jdbc.JdbcClient;
import com.flipkart.fdp.superbi.refresher.dao.jdbc.JdbcDataSourceDao;
import org.apache.tomcat.dbcp.dbcp.ConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.DriverManagerConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolableConnectionFactory;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;
import org.apache.tomcat.dbcp.pool.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class VerticaDataSourceFactory implements DataSourceFactory {

    private static final long VERTICA_QUERY_TIMEOUT = 300000;
    private static final Integer MAX_CONNECTIONS = 10;

    @Override
    public DataSourceDao getDao(Map<String, String> attributes) {
        String jdbcUrl = attributes.get("jdbcUrl");
        Integer maxConnections = attributes.get("max_active_connections") != null ? Integer.valueOf(attributes.get("max_active_connections")) : MAX_CONNECTIONS;
        Preconditions.checkNotNull(jdbcUrl);
        Properties properties = new Properties();
        properties.putAll(attributes);
        return new JdbcDataSourceDao(getJdbcClient(jdbcUrl,properties,maxConnections,VERTICA_QUERY_TIMEOUT,new ArrayList<>()));
    }

    @Override
    public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
        return new VerticaDSLConfig(dslConfig);
    }

    private static JdbcClient getJdbcClient(String jdbcUrl, Properties properties, Integer maxActiveConnections, long timeoutInMs, List<String> initScripts
    ) {
        final ConnectionFactory cf = new DriverManagerConnectionFactory(
                jdbcUrl,
                properties);

        return JdbcConnectionPoolCreator (cf, maxActiveConnections,timeoutInMs,initScripts);
    }

    private static JdbcClient JdbcConnectionPoolCreator(ConnectionFactory cf,Integer maxActiveConnections,long timeoutInMs,List<String> initScripts) {

        GenericObjectPool connectionPool = new GenericObjectPool();
        connectionPool.setTestOnBorrow(false);
        connectionPool.setTestOnReturn(false);
        connectionPool.setTestWhileIdle(true);
        connectionPool.setTimeBetweenEvictionRunsMillis(60 * 1000L);
        connectionPool.setMaxActive(maxActiveConnections);


        new PoolableConnectionFactory(cf, connectionPool, null, "SELECT 1", false, false);
        PoolingDataSource dataSource = new PoolingDataSource(connectionPool);

        return new JdbcClient(dataSource, (int) (timeoutInMs/1000), initScripts);

    }



}
