package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by abhijat.chowdhary on 10/08/15.
 */
public class QueryTimeoutException extends QueryExecutionException {
    public QueryTimeoutException() {
    }

    public QueryTimeoutException(String message) {
        super(message);
    }

    public QueryTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryTimeoutException(Throwable cause) {
        super(cause);
    }
}
