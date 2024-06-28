package com.flipkart.fdp.superbi.refresher.api.exception;

public class CanNotRemoveCacheException extends CacheException{

    public CanNotRemoveCacheException() {
    }

    public CanNotRemoveCacheException(String message) {
        super(message);
    }

    public CanNotRemoveCacheException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotRemoveCacheException(Throwable cause) {
        super(cause);
    }
}
