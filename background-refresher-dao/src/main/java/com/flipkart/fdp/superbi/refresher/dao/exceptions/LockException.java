package com.flipkart.fdp.superbi.refresher.dao.exceptions;

public class LockException extends RuntimeException{

    public LockException() {
    }

    public LockException(String message) {
        super(message);
    }

    public LockException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockException(Throwable cause) {
        super(cause);
    }
    
}
