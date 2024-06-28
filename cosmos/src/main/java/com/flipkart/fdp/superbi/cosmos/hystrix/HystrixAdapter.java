package com.flipkart.fdp.superbi.cosmos.hystrix;


import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

/**
 * Created by amruth.s on 28/12/14.
 */
public class HystrixAdapter implements RemoteCallExecutor {

    private static final Logger logger = LoggerFactory.getLogger(HystrixAdapter.class);

    private <T> HystrixCommand<T> hystrixRequest(final RemoteCall<T> remoteCall) {
        /**
         * Hystrix does not expose any facades for creating new thread/cmd groups
         * It gets implicitly created during the command invocation if needed
         * So the thread pool properties gets picked up only when it is created for the first time
         *
         */
        final String serviceGroupName = remoteCall.getServiceGroupName();
        final String serviceGroupSubName = remoteCall.getServiceGroupSubName();
        final ServiceConfigDefaults.Config serviceConfig = ServiceConfigDefaults.getByGroupName(serviceGroupName);

        HystrixThreadPoolProperties.Setter threadPoolProperties = HystrixThreadPoolProperties.Setter()
                .withCoreSize(serviceConfig.getMaxThreads());

        if(serviceConfig.getMaxQueueSize().isPresent())
        {
            threadPoolProperties =  threadPoolProperties.withMaxQueueSize(serviceConfig.getMaxQueueSize().get());
        }
        if(serviceConfig.getRejectionLimit().isPresent())
        {
            threadPoolProperties =  threadPoolProperties.withQueueSizeRejectionThreshold(serviceConfig.getRejectionLimit().get());

        }
        String hystrixKey = serviceGroupName + "_" + serviceGroupSubName;

        return new HystrixCommand<T>(
                HystrixCommand.Setter
                        .withGroupKey(
                                HystrixCommandGroupKey.Factory.asKey(hystrixKey)
                        )
                        .andCommandPropertiesDefaults(
                                HystrixCommandProperties.Setter()
                                        .withExecutionTimeoutInMilliseconds(remoteCall.getTimeOut())
                        )
                        .andCommandKey(HystrixCommandKey.Factory.asKey(hystrixKey))
                        .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(hystrixKey))
                        .andThreadPoolPropertiesDefaults(threadPoolProperties)
        ) {
            @Override
            protected T run() throws Exception {
                try {
                    return remoteCall.getActualCall().workUnit();
                } catch (Exception e) {
                    /**
                     * Hystrix swallows the exception and prints its own message..
                     * So logging it before throwing
                     */
                    logger.error("Hystrix execution failed - ", e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected T getFallback() {

                return remoteCall.getFallBackCall().workUnit();
            }
        };
    }

    private <T> HystrixObservableCommand<T> hystrixRequestObservable(final RemoteCall<T> remoteCall) {
        /**
         * Hystrix does not expose any facades for creating new thread/cmd groups
         * It gets implicitly created during the command invocation if needed
         * So the thread pool properties gets picked up only when it is created for the first time
         *
         */
        final String serviceGroupName = remoteCall.getServiceGroupName();
        final String serviceGroupSubName = remoteCall.getServiceGroupSubName();
        final ServiceConfigDefaults.Config serviceConfig = ServiceConfigDefaults.getByGroupName(serviceGroupName);

        HystrixThreadPoolProperties.Setter threadPoolProperties = HystrixThreadPoolProperties.Setter()
            .withCoreSize(serviceConfig.getMaxThreads());

        if(serviceConfig.getMaxQueueSize().isPresent())
        {
            threadPoolProperties =  threadPoolProperties.withMaxQueueSize(serviceConfig.getMaxQueueSize().get());
        }
        if(serviceConfig.getRejectionLimit().isPresent())
        {
            threadPoolProperties =  threadPoolProperties.withQueueSizeRejectionThreshold(serviceConfig.getRejectionLimit().get());

        }
        String hystrixKey = serviceGroupName + "_" + serviceGroupSubName;

        HystrixCommandGroupKey groupKey = HystrixCommandGroupKey.Factory.asKey(hystrixKey);

        HystrixObservableCommand.Setter setter = HystrixObservableCommand.Setter
            .withGroupKey(
                HystrixCommandGroupKey.Factory.asKey(hystrixKey)
            )
            .andCommandPropertiesDefaults(
                HystrixCommandProperties.Setter()
                    .withExecutionTimeoutInMilliseconds(remoteCall.getTimeOut())
            )
            .andCommandKey(HystrixCommandKey.Factory.asKey(hystrixKey));

        // TODO
//            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey(serviceGroupName))
//            .andThreadPoolPropertiesDefaults(threadPoolProperties);

        return new HystrixObservableCommand<T>(setter) {
            @Override
            protected Observable<T> construct() {
                return Observable.create(new Observable.OnSubscribe<T>() {

                    @Override
                    public void call(Subscriber<? super T> observer) {
                        try {
                            if (!observer.isUnsubscribed()) {
                                T result = remoteCall.getActualCall().workUnit();
                                observer.onNext(result);
                                observer.onCompleted();
                            }else {
                                throw new RuntimeException("Subscriber not available");
                            }
                        } catch (Exception e) {
                            /**
                             * Hystrix swallows the exception and prints its own message..
                             * So logging it before throwing
                             */
                            logger.error("Hystrix execution failed - ", e);
                            observer.onError(e);
                        }
                    }
                } ).subscribeOn(Schedulers.io());
            }
        };
    }


    @Override
    public <T> T execute(RemoteCall<T> remoteCall) {
        return hystrixRequest(remoteCall).execute();
    }

    @Override
    public <T> Future<T> queue(RemoteCall<T> remoteCall) {
        return hystrixRequest(remoteCall).queue();
    }

    @Override
    public <T> Observable<T> observe(RemoteCall<T> remoteCall) {
        return hystrixRequest(remoteCall).observe();
    }
}
