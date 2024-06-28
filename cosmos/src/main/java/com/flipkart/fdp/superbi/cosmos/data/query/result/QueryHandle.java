package com.flipkart.fdp.superbi.cosmos.data.query.result;

import com.flipkart.fdp.superbi.cosmos.data.query.QuerySubmitResult;
import java.io.Serializable;

/**
 * Created by arun.khetarpal on 25/11/15.
 */
public class QueryHandle extends QuerySubmitResult implements Serializable {
    private String handle;
    private long pollingFrequencyInSec;

    public static class Builder {
        private QueryHandle instance;

        public Builder() { instance = new QueryHandle(); }

        public Builder setHandle (String handle) {
            instance.handle = handle;
            return this;
        }

        public Builder setPollingFrequencyInSec (long pollingFrequencyInSec) {
            instance.pollingFrequencyInSec = pollingFrequencyInSec;
            return this;
        }

        public QueryHandle build() { return this.instance; }
    }

    public String getHandle() {
        return handle;
    }

    public long getPollingFrequencyInSec() {return pollingFrequencyInSec; }
}
