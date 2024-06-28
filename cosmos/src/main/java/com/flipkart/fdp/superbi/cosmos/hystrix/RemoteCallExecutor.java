package com.flipkart.fdp.superbi.cosmos.hystrix;

import java.util.concurrent.Future;

/**
 *
 * Created by amruth.s on 28/12/14.
 */
public interface RemoteCallExecutor {
    <T> T execute(RemoteCall<T> remoteCall);
    <T> Future<T> queue(RemoteCall<T> remoteCall);
    <T> rx.Observable<T> observe(RemoteCall<T> remoteCall);
}
