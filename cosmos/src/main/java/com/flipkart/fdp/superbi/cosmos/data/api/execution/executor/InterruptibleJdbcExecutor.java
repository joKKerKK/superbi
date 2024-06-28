package com.flipkart.fdp.superbi.cosmos.data.api.execution.executor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by arun.khetarpal on 09/09/15.
 */
public class InterruptibleJdbcExecutor implements QueryExecutor, Runnable {
    private Thread worker;
    private Params params;
    private Results results;
    private volatile boolean cancelRequest;
    private volatile boolean closeRequest;

    private class Params {
        public Statement statement = null;
        public String query = null;
        public boolean synced = false;
    }

    private class Results {
        public ResultSet rs = null;
        public SQLException exception = null;
        public boolean serviced = false;
    }

    public InterruptibleJdbcExecutor() {
        params = new Params();
        results = new Results();
        worker = new Thread(this);
        worker.start();
    }

    /**
     * Executes an SQL query.
     * The method can be interrupted by another thread at any moment.
     * @return <code>ResultSet</code> if execution successful
     * @exception SQLException if a database error occurs
     * @see {@link Statement#executeQuery(String)}
     **/
    public synchronized ResultSet executeQuery(Statement statement, String query)
            throws SQLException {

        synchronized(params) {
            params.statement = statement;
            params.query = query;
            params.synced = true;
            params.notify();
        }

        synchronized(results) {
            try {
                while(!results.serviced)
                    results.wait();
                if (results.exception != null)
                    throw results.exception;
            } catch (InterruptedException e) {
                cancel();
                throw new SQLException(e);
            }
            return results.rs;
        }
    }

    private void cancel() throws SQLException {
        cancelRequest = true;

        try {
            params.statement.cancel();
            synchronized(results) {
                while(!results.serviced)
                    results.wait();
            }
        } catch (SQLException | InterruptedException e) {
            return;
        }
    }

    @Override
    public void close() throws SQLException {
        closeRequest = true;

        worker.interrupt();
        if (params.statement != null)
            cancel();
        try {
            worker.join();
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
    }

    public void run() {

        if (closeRequest || cancelRequest) return;

        ResultSet rs = null;
        SQLException ex = null;

        synchronized (params) {
            try {
                while (!params.synced)
                    params.wait();
                params.synced = false;
            } catch (InterruptedException e) {
                return;
            }
            try {
                rs = params.statement.executeQuery(params.query);
            } catch (SQLException e) {
                ex = e;

            }
            finally {
                synchronized (results) {
                    results.rs = rs;
                    results.exception = ex;
                    results.serviced = true;
                    results.notify();
                }
            }
        }
    }
}
