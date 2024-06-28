package com.flipkart.fdp.superbi.cosmos.hystrix;

/**
 * Created by amruth.s on 28/12/14.
 */


public abstract class ActualCall<T> {
    public abstract T workUnit();
}
