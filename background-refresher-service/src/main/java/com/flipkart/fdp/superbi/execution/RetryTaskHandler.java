package com.flipkart.fdp.superbi.execution;

public interface RetryTaskHandler {
    void submitForRetry(BackgroundRefreshTask task);
}
