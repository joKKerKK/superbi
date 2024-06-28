package com.flipkart.fdp.superbi.refresher.api.exception;

public class CanNotSetCacheException extends CacheException{

    public CanNotSetCacheException() {
    }

    public CanNotSetCacheException(String message) {
        super(message);
    }

    public CanNotSetCacheException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotSetCacheException(Throwable cause) {
        super(cause);
    }
}
