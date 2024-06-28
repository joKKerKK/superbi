package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.asyncImpl;

import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResultMeta;
import java.util.Optional;

/**
 * Observes the class {@link AsyncTask} and takes actions based on state changes,
 * failure notifications or results availability.
 *
 * @author arun.khetarpal
 */
public abstract class Observer {
    protected AsyncTask<Void> subject;

    public Observer(AsyncTask<Void> subject) {
        this.subject = subject;
    }

    public void notifyStateChange(QueryResultMeta.QueryStatus state) {}
    public void notifyResults(Optional<QueryResult> results) {}
    public void notifyFailure(Exception ex) {}
}
