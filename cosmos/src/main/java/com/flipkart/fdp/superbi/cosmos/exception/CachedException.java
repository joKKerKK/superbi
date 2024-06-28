package com.flipkart.fdp.superbi.cosmos.exception;

/**
 * Created by rajesh.kannan on 26/09/16.
 */
public class CachedException extends RuntimeException
{
    public String stackTrace = null;
    public String cacheKey = null;
    public CachedException(String message)
    {
        super(message);
    }

    public CachedException(String message, String stackTrace, String cacheKey)
    {
        this(message);
        this.stackTrace = stackTrace;
        this.cacheKey = cacheKey;
    }

    @Override
    public String toString()
    {
        return String.format("cacheKey=%s, message=%s, additional stacktrace=%s", cacheKey, getMessage(), stackTrace);
    }
}
