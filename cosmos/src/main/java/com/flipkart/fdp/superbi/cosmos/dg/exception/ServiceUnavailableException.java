package com.flipkart.fdp.superbi.cosmos.dg.exception;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public class ServiceUnavailableException extends RuntimeException {
    public ServiceUnavailableException() {}
    public ServiceUnavailableException(String message) {
        super(message);
    }
    public ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
    public ServiceUnavailableException(Throwable cause) {
        super(cause);
    }
}
