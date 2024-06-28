package com.flipkart.fdp.superbi.cosmos.data.api.execution.jdbc;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.executor.InterruptibleJdbcExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.executor.QueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.dbcp.dbcp.PoolingDataSource;

import java.io.PrintStream;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Slf4j
public class JdbcDao {

    private final PoolingDataSource dataSource;
    private final int timeOutSec;
    private final List<String> initScripts;

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public JdbcDao(PoolingDataSource dataSource, int timeOutSec) {
        this(dataSource, timeOutSec, Lists.newArrayList());
    }

    public JdbcDao(PoolingDataSource dataSource, int timeOutSec,List<String> initScripts) {
        this.dataSource = dataSource;
        this.timeOutSec = timeOutSec;
        this.initScripts = initScripts;
    }

    public JdbcStreamingQueryResult getStreamingIterable(String query) {
        try {
            return new JdbcStreamingQueryResult(getConnection(), query, initScripts );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute the query on the {@link #dataSource}.
     * @return Processed results
     * @exception RuntimeException Decorates SQLException into RuntimeException
     * @todo Decouple the concrete InterruptableJdbcExecutor implementation
     * @todo Currently {@link ResultSet} throws AbstractMethod Exception
     */
    @LogExecTime
    public QueryResult execute(String query) {
        try (Connection connection = dataSource.getConnection()) {
            executeInitScripts(connection, initScripts, of(timeOutSec));
            try (Statement statement = connection.createStatement()) {
                try (QueryExecutor queryExecutor = new InterruptibleJdbcExecutor()) {
                    ResultSet resultSet = queryExecutor.executeQuery(statement, query);

                    final ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    List<String> schema = Lists.newArrayList();
                    List<ResultRow> rows = Lists.newArrayList();
                    for (int i = 1; i < columnCount + 1; i++) {
                        schema.add(metaData.getColumnName(i));
                    }
                    while (resultSet.next()) {
                        ResultRow row = new ResultRow();
                        for (int i = 1; i < columnCount + 1; i++) {
                            row.row.add(resultSet.getObject(i));
                        }
                        rows.add(row);
                    }
                    return new QueryResult(null, rows);
                }
            }
        } catch (SQLException se) {
            se.printStackTrace(new PrintStream(System.out));
            throw new RuntimeException(se);
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



    public Object explain(Object nativeQuery) {
        return execute("explain " + nativeQuery);
    }

    private static class JdbcStreamingQueryResult extends StreamingQueryResult {

        private ResultSet resultSet;
        private Connection connection;
        private Statement stmt;
        private List<String> schema = Lists.newArrayList();
        private static enum iterationState { BEFORE_START, DURING, AFTER_END}
        private iterationState state = iterationState.BEFORE_START;

        @Override
        public void close() {
//            tryCloseResultSet(); TODO throws abstract method error
            tryCloseStatement(stmt);
            tryCloseConnection(connection);
        }

        public JdbcStreamingQueryResult(Connection connection, String query, List<String> initScripts) {
            try {
                this.connection = connection;
                executeInitScripts(connection, initScripts, empty());
                this.stmt = connection.createStatement();
                this.resultSet = stmt.executeQuery(query);
                final ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i < columnCount + 1; i++ ) {
                  this.schema.add(metaData.getColumnName(i));
                }
            } catch (SQLException e) {
                close();
                log.error("Streaming failed", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<List<Object>> iterator() {
            return new Iterator<List<Object>>() {
                @Override
                public boolean hasNext() {
                    try {
                        if(state == iterationState.BEFORE_START) {
                            /**
                             * Damn! Vertica have limitations. 
                             1. It does not support bidirectional cursor.
                             2. It also does not respect JDBC specification where isBeforeFirst() 
                             and isAfterLast() returns false if ResultSet is empty.
                             **/
                            if(resultSet.next() == false) {
                                close();
                                return false;
                            }
                            state = iterationState.DURING;
                            return true;
                        }
                        return state == iterationState.DURING ? true : false;

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public List<Object> next() {
                    try {
                        final int columnCount = schema.size();
                        final List<Object> row = Lists.newArrayList();
                        for(int i = 1; i<=columnCount;i++)
                            row.add(resultSet.getObject(i)) ;
                        if(resultSet.next() == false) {
                            state = iterationState.AFTER_END;
                            close();
                        }
                        return row;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("This is a read only set, " +
                            "Remove operation is not supported");
                }
            };
        }

        @Override
        public Schema getSchema() {
            return null;
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
