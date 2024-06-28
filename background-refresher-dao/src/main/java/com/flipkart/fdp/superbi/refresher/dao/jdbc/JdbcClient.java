package com.flipkart.fdp.superbi.refresher.dao.jdbc;

import static java.util.Optional.empty;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;

@Slf4j
public class JdbcClient {

    private final PoolingDataSource dataSource;
    private final int timeOutSec;
    private final List<String> initScripts;

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public JdbcClient(PoolingDataSource dataSource, int timeOutSec) {
        this(dataSource, timeOutSec, Lists.newArrayList());
    }

    public JdbcClient(PoolingDataSource dataSource, int timeOutSec, List<String> initScripts) {
        this.dataSource = dataSource;
        this.timeOutSec = timeOutSec;
        this.initScripts = initScripts;
    }

    public SqlQueryResult getStreamingIterable(String query) {
        try {
            return new SqlQueryResult(getConnection(), query, initScripts );
        } catch (SQLSyntaxErrorException | SQLTransientException jdbcClientException) {
            log.warn("Execution failed for {} due to {}", query, jdbcClientException.getMessage());
            throw new ClientSideException(jdbcClientException);
        }
        catch (Throwable t) {
            log.warn("Execution failed for {} due to {}", query, t.getMessage());
            throw new ServerSideException(t);
        }
    }

    private static void executeInitScripts(Connection connection, List<String> initScripts,Optional<Integer> timeOutSec ) throws SQLException {

        if(initScripts.isEmpty())
            return;

        log.info("Executing initscripts ");

        for(String initScript : initScripts) {
            Statement initStatement = connection.createStatement();
            setTimeoutIfPresent(initStatement, timeOutSec);
            initStatement.executeUpdate(initScript);
        }

    }
    private static void setTimeoutIfPresent(Statement statement, Optional<Integer> timeOutSec) {
        try {
            if(timeOutSec.isPresent())
			    statement.setQueryTimeout(timeOutSec.get());
		}
		catch (SQLException e) {
			log.warn("timeout not supported in Jdbc driver");
		}
    }


    private static class SqlQueryResult extends
        com.flipkart.fdp.superbi.refresher.dao.result.JdbcQueryResult {
        private final Statement stmt;

        @Override
        public void close() {
            tryCloseStatement(stmt);
            tryCloseConnection(connection);
        }

        @SneakyThrows
        private SqlQueryResult(Connection connection, String query, List<String> initScripts) {
            try {
                this.connection = connection;
                executeInitScripts(connection, initScripts, empty());
                this.stmt = connection.createStatement();
                this.resultSet = stmt.executeQuery(query);
                final ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i < columnCount + 1; i++ ) {
                  this.schema.add(metaData.getColumnLabel(i));
                }
            } catch (SQLException e) {
                close();
                throw e;
            }
        }
    }

    private static void tryCloseConnection (Connection connection) {
        try {
            if(connection != null && !connection.isClosed())
                connection.close();
        } catch (SQLException e) {
            log.error("Cannot close connection",e);
        }
    }

    private static void tryCloseStatement (Statement stmt) {
        try {
            if(stmt != null && !stmt.isClosed())
                stmt.close();
        } catch (SQLException e) {
            log.error("Cannot close stmt", e);
        }
    }

    private static void tryCloseResultSet (ResultSet resultSet) {
        try {
            if(resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
        } catch (SQLException e) {
            log.error("Cannot close resultSet", e);
        }
    }

}
