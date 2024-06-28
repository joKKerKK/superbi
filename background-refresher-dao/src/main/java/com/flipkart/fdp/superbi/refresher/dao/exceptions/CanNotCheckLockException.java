package com.flipkart.fdp.superbi.refresher.dao.exceptions;

public class CanNotCheckLockException extends LockException{

    public CanNotCheckLockException() {
    }

    public CanNotCheckLockException(String message) {
        super(message);
    }

    public CanNotCheckLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotCheckLockException(Throwable cause) {
        super(cause);
    }
}
