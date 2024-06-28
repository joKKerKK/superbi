package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl.DistributedLock
    .getAtomicDistributedLock;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ServerSideTransformer;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.NativeQueryTranslator;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryHandle;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * The actual 'Work horse' call in async. Updates all the 'Subscribers' for various changes
 * that may happen over the course of its working
 * @param <Void>
 *
 * @author arun.khetarpal
 */
public class AsyncTask<Void> extends ActualCall {

    private ExecutionContext       context;
    private NativeQueryTranslator  translator;
    private QueryHandle            handle;
    private long                   factCreatedAtTime;
    private int                    lockGraceTime;
    private long                   resultExpireTime;
    private List<Observer> observers = new ArrayList<Observer>();

    public AsyncTask(ExecutionContext context, NativeQueryTranslator translator, QueryHandle handle,
                     long factCreatedAtTime, int lockGraceTime, long resultExpireTime) {
        this.context = context;
        this.translator = translator;
        this.handle = handle;
        this.factCreatedAtTime = factCreatedAtTime;
        this.lockGraceTime = lockGraceTime;
        this.resultExpireTime = resultExpireTime;
    }

    public void attach(Observer observer) {
        observers.add(observer);
    }

    protected void updateState(QueryResultMeta.QueryStatus state) {
        notifyStateChange(state);
    }

    protected void updateResults(Optional<QueryResult> results) {
        notifyResultsAvailable(results);
    }

    protected void updateFailure(Exception ex) {
        notifyFailure(ex);
    }

    public ExecutionContext getContext() {
        return context;
    }

    public NativeQueryTranslator getTranslator() {
        return translator;
    }

    public QueryHandle getHandle() {
        return handle;
    }

    public long getFactCreatedAtTime() { return factCreatedAtTime; }

    public long getResultExpireTime() { return resultExpireTime; }

    public List<Observer> getObservers() {
        return observers;
    }

    @Override
    public Void workUnit() {
        DSQuery dsQuery = context.getQuery();
        Object nativeQuery = translator.getTranslatedQuery();
        Map<String, String[]> params = context.getParams();

        try (DistributedLock lock = getAtomicDistributedLock(getDistributedLockStore(),
                             handle.getHandle(), getId(), lockGraceTime)) {

            lock.lock(lockGraceTime, getId());

            updateState(QueryResultMeta.QueryStatus.SUBMITTED);
            DSQueryExecutor executor = context.getExecutor();

            updateState(QueryResultMeta.QueryStatus.RUNNING);
            QueryResult rawResults = executor.executeNative(nativeQuery,
                    new DSQueryExecutor.ExecutionContext(dsQuery, params));

            updateState(QueryResultMeta.QueryStatus.POST_PROCESSING);
            Optional<QueryResult> results = Optional.ofNullable(
                    ServerSideTransformer.getFor(dsQuery, params, null).postProcess(rawResults));

            updateResults(results);
            updateState(QueryResultMeta.QueryStatus.SUCCESSFUL);

        } catch (Exception ex) {
            updateState(QueryResultMeta.QueryStatus.FAILED);
            updateFailure(ex);
        }

        return null;
    }

    private ICacheClient<String, String> getDistributedLockStore() {
        return context.getDistributedLockStoreOptional().get();
    }

    private void notifyStateChange(QueryResultMeta.QueryStatus state) {
        for (Observer observer: observers)
            observer.notifyStateChange(state);
    }

    private void notifyResultsAvailable(Optional<QueryResult> results) {
        for (Observer observer: observers)
            observer.notifyResults(results);
    }

    private void notifyFailure(Exception ex) {
        for (Observer observer: observers)
            observer.notifyFailure(ex);
    }

    private String getId() {
        try {
            return String.format("%s_%s", InetAddress.getLocalHost().toString(), Thread.currentThread().getName());
        } catch(UnknownHostException uhe) {
            return String.format("Unknown_%s", Thread.currentThread().getId());
        }
    }
}