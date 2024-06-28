package com.flipkart.fdp.superbi.refresher.dao;

import com.flipkart.fdp.superbi.refresher.dao.jdbc.JdbcClient;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;

import java.sql.*;
import java.util.Arrays;

@RunWith(PowerMockRunner.class)
public class JdbcClientTest {

    @Mock
    private PoolingDataSource poolingDataSource;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;
    @Mock
    private ResultSetMetaData resultSetMetaData;

    private static String SAMPLE_QUERY = "select * from forward_unit_vertica_fact";

    @Test
    public void testStreamingResultSuccessWithSingleColumn() throws SQLException {
        Mockito.when(poolingDataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(SAMPLE_QUERY)).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(1);
        Mockito.when(resultSetMetaData.getColumnLabel(Mockito.anyInt())).thenReturn("gmv");
        /**
         * Mock only one row
         */
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        Mockito.when(resultSet.getObject(Mockito.anyInt())).thenReturn("data");

        JdbcClient jdbcClient = new JdbcClient(poolingDataSource,10);
        QueryResult queryResult = jdbcClient.getStreamingIterable(SAMPLE_QUERY);

        Assert.assertEquals(queryResult.iterator().hasNext(),true);
        Assert.assertEquals(queryResult.iterator().next(),Arrays.asList("data"));
        Assert.assertEquals(queryResult.iterator().hasNext(),false);
        Assert.assertEquals(queryResult.getColumns().get(0),resultSetMetaData.getColumnLabel(0));
    }

    @Test
    public void testStreamingResultSuccessWithMultipleColumn() throws SQLException {
        Mockito.when(poolingDataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(SAMPLE_QUERY)).thenReturn(resultSet);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);
        Mockito.when(resultSetMetaData.getColumnLabel(Mockito.anyInt())).thenReturn("columnName");
        /**
         * Mock only one row
         */
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

        Mockito.when(resultSet.getObject(Mockito.anyInt())).thenReturn("data");

        JdbcClient jdbcClient = new JdbcClient(poolingDataSource,10);
        QueryResult queryResult = jdbcClient.getStreamingIterable(SAMPLE_QUERY);

        Assert.assertEquals(queryResult.iterator().hasNext(),true);
        Assert.assertEquals(queryResult.iterator().next(),Arrays.asList("data","data"));
        Assert.assertEquals(queryResult.iterator().hasNext(),false);
        Assert.assertEquals(queryResult.getColumns().get(0),resultSetMetaData.getColumnLabel(0));
        Assert.assertEquals(queryResult.getColumns().get(1),resultSetMetaData.getColumnLabel(1));
    }

    @Test(expected = RuntimeException.class)
    public void testStreamingResultWithException() throws SQLException {
        Mockito.when(poolingDataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(statement.executeQuery(SAMPLE_QUERY)).thenThrow(new SQLException());
        JdbcClient jdbcClient = new JdbcClient(poolingDataSource,10);
        QueryResult queryResult = jdbcClient.getStreamingIterable(SAMPLE_QUERY);
    }

}
