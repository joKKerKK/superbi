package com.flipkart.fdp.superbi.cosmos.hystrix;

import java.util.concurrent.Future;

/**
 *
 * Created by amruth.s on 26/12/14.
 *
 * Wrap the work into service call for
 * - synchronous
 * - asynchronous and
 * - reactive execution
 *
 *
 */
public class RemoteCall<T> {

    private int timeOut = 10000;
    private String serviceGroupName;
    private String serviceGroupSubName;
    private ActualCall<T> actualCall;
    private ActualCall<T> fallBackCall;
    /**
     * Hard coded hystrix here, decouple if necessary
     */
    private static RemoteCallExecutor callExecutor = new HystrixAdapter();

    private RemoteCall() {}

    public static class Builder<T> {
        RemoteCall<T> call = new RemoteCall<T>();

        public Builder (String serviceName) {
            this(serviceName, "");
        }

        public Builder (String serviceName, String serviceGroupSubName) {
            call.serviceGroupName = serviceName;
            call.serviceGroupSubName = serviceGroupSubName;
            call.fallBackCall = new ActualCall<T>() {
                @Override
                public T workUnit() {
                    throw new UnsupportedOperationException("Fallback not available");
                }
            };
        }

        public Builder<T> withTimeOut(int timeOut) {
            call.timeOut = timeOut;
            return this;
        }

        public Builder<T> withFallBack(ActualCall<T> fallBackCall) {
            call.fallBackCall = fallBackCall;
            return this;
        }

        public RemoteCall<T> around(ActualCall<T> actualCall) {
            call.actualCall = actualCall;
            return call;
        }

    }

    public T execute() {
        return callExecutor.execute(this);
    }

    public Future<T> executeAsync() {
        return callExecutor.queue(this);
    }

    public rx.Observable<T> executeReactive() {
        return callExecutor.observe(this);
    }

    public int getTimeOut() {
        return timeOut;
    }

    public String getServiceGroupName() {
        return serviceGroupName;
    }

    public String getServiceGroupSubName() {
       return serviceGroupSubName;
    }

    public ActualCall<T> getActualCall() {
        return actualCall;
    }

    public ActualCall<T> getFallBackCall() {
        return fallBackCall;
    }
}
