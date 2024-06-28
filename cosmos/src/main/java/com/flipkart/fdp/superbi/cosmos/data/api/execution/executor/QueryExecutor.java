package com.flipkart.fdp.superbi.cosmos.data.api.execution.executor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by arun.khetarpal on 09/09/15.
 */
public interface QueryExecutor extends AutoCloseable {
    default public ResultSet executeQuery(Statement stmt, String query) throws SQLException {
        return stmt.executeQuery(query);
    }
    abstract public void close() throws SQLException;
}
