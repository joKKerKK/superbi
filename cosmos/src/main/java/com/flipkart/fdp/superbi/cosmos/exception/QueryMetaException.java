package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by chandrasekhar.v on 26/11/15.
 */
public class QueryMetaException extends RuntimeException {

    public QueryMetaException(String message) {
        super(message);
    }

    public QueryMetaException(String message, Throwable cause) {
        super(message, cause);
    }
}
