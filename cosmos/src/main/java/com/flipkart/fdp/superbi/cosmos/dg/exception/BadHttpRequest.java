package com.flipkart.fdp.superbi.cosmos.dg.exception;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public class BadHttpRequest extends RuntimeException {
    public BadHttpRequest() {}
    public BadHttpRequest(String message) {
        super(message);
    }
    public BadHttpRequest(String message, Throwable cause) {
        super(message, cause);
    }
    public BadHttpRequest(Throwable cause) {
        super(cause);
    }
}
