package com.flipkart.fdp.superbi.refresher.api.exception;

public class CanNotReadCacheException extends CacheException{

    public CanNotReadCacheException() {
    }

    public CanNotReadCacheException(String message) {
        super(message);
    }

    public CanNotReadCacheException(String message, Throwable cause) {
        super(message, cause);
    }

    public CanNotReadCacheException(Throwable cause) {
        super(cause);
    }
    
}
