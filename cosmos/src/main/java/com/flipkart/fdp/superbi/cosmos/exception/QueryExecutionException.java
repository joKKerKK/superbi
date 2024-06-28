package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by abhijat.chowdhary on 10/08/15.
 */
public class QueryExecutionException extends RuntimeException {

    public QueryExecutionException() {
    }

    public QueryExecutionException(String message) {
        super(message);
    }

    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryExecutionException(Throwable cause) {
        super(cause);
    }
}
