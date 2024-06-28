package com.flipkart.fdp.superbi.cosmos.dg.exception;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public class InternalErrorException extends RuntimeException {
    public InternalErrorException() {}
    public InternalErrorException(String message) {
        super(message);
    }
    public InternalErrorException(String message, Throwable cause) {
        super(message, cause);
    }
    public InternalErrorException(Throwable cause) {
        super(cause);
    }
}
