package com.flipkart.fdp.superbi.execution;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import com.google.common.collect.Maps;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@AllArgsConstructor
public class FailureStreamConsumer implements BiConsumer<BackgroundRefreshTask, Throwable>{

    private static final String FAILURE_COUNTER_KEY = "failure.stream.counter" ;
    private final CacheDao cacheDao;
    private final MetricRegistry metricRegistry;

    @Override
    public void accept(BackgroundRefreshTask task, Throwable e) {
        log.info("Executing failure consumer for task with requestId - <{}> and cacheKey - <{}>", task.getQueryPayload().getRequestId(),task.getCacheKey());
        Counter failureCounter = getFailureCounter(task.getQueryPayload().getStoreIdentifier());
        failureCounter.inc();
        boolean isServerSideException = true;
        boolean isRetryable = true;
        if(task.getRemainingRetry() == 0 && !(e instanceof CallNotPermittedException) &&
            !(e instanceof BulkheadFullException) ){
            e = new ClientSideException(e);
            isRetryable = false;
            isServerSideException =false;
        }
        if(e instanceof ClientSideException) {
            isRetryable = false;
            isServerSideException =false;
        }

        //Task has been rejected by resilliance4j.
        if (e instanceof BulkheadFullException || e instanceof CallNotPermittedException) {
            isServerSideException = true;
        }

        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));

        long currentTime = new Date().getTime();

        Map<String, String> _debugInfo = Maps.newHashMap();
        _debugInfo.put("MAX_RETRIES",
                String.valueOf(task.getBackgroundRefresherConfig().getNumOfRetryOnException()));
        _debugInfo.put("STACKTRACE", writer.toString());

        AttemptInfo attemptInfo = AttemptInfo.builder()
                .startedAtTime(currentTime)
                .finishedAtTime(currentTime)
                .cachedAtTime(currentTime)
                .errorMessage(e.getMessage())
                .status(AttemptInfo.Status.FAILED)
                .serverError(isServerSideException)
                .requestId(task.getQueryPayload().getRequestId())
                .attemptKey(task.getQueryPayload().getAttemptKey())
                .cacheKey(task.getQueryPayload().getCacheKey())
                .refresherNodeAddress(getHostName())
                ._debugInfo(_debugInfo)
                .build();

        cacheDao.set(task.getQueryPayload().getAttemptKey(),
                task.getBackgroundRefresherConfig().getResultCacheTtlInSec(),attemptInfo);;
        //Task has failed, and circuit is not open; Let's see if we can retry
        if (task.getRemainingRetry() > 0 && isRetryable) {
            log.info("Submitting task for retry with requestId - <{}> and attemptNumber - <{}> and cacheKey- <{}>",
                    task.getQueryPayload().getRequestId(),task.getAttemptNumber(),task.getCacheKey());
            task.submitForRetry();
        }else {
            task.unLockTask();
        }
    }

    private static String getHostName() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "Unknown";
        }
    }

    private Counter getFailureCounter(String storeIdentifier) {
        return metricRegistry.counter(getMetricsKey(FAILURE_COUNTER_KEY,storeIdentifier));
    }

    private String getMetricsKey(String prefix, String storeIdentifier) {
        return StringUtils.join(Arrays.asList(prefix,storeIdentifier),'.');
    }
}