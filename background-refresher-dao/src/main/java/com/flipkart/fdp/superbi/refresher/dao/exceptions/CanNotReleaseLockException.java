package com.flipkart.fdp.superbi.refresher.dao.exceptions;

public class CanNotReleaseLockException extends LockException{
    
    public CanNotReleaseLockException() {
    }

    public CanNotReleaseLockException(String message) {
        super(message);
    }

    public CanNotReleaseLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotReleaseLockException(Throwable cause) {
        super(cause);
    }
}
