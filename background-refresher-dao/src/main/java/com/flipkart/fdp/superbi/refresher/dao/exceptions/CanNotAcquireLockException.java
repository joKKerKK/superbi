package com.flipkart.fdp.superbi.refresher.dao.exceptions;

public class CanNotAcquireLockException extends LockException{

    public CanNotAcquireLockException() {
    }

    public CanNotAcquireLockException(String message) {
        super(message);
    }

    public CanNotAcquireLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotAcquireLockException(Throwable cause) {
        super(cause);
    }
}
