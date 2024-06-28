package com.flipkart.fdp.superbi.execution.exception;

public class DataSourceNotFoundException extends RuntimeException{

    public DataSourceNotFoundException() {
    }

    public DataSourceNotFoundException(String message) {
        super(message);
    }

    public DataSourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataSourceNotFoundException(Throwable cause) {
        super(cause);
    }

}
