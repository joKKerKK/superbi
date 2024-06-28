package com.flipkart.fdp.superbi.cosmos.data.query.result;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.cosmos.data.query.QuerySubmitResult;
import java.io.Serializable;
import java.util.Map;
import lombok.ToString;

/**
 * Created by chandrasekhar.v on 25/11/15.
 */
@JsonFilter("QueryMetaFilter")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@ToString
public class QueryResultMeta extends QuerySubmitResult implements Serializable {

    public enum QueryStatus {
        SUBMITTED,
        RUNNING,
        POST_PROCESSING,
        SUCCESSFUL,
        FAILED,
        INVALID
    }

    @JsonProperty
    private String threadId;
    @JsonProperty
    private String hostName;
    @JsonProperty
    private String errorMessage;
    @JsonProperty
    private QueryStatus status;
    @JsonProperty
    private boolean cached;
    @JsonProperty
    private long submittedAtTime;
    @JsonProperty
    private long executionStartTime;
    @JsonProperty
    private long executionEndTime;
    @JsonProperty
    private long processingStartTime;
    @JsonProperty
    private long processingEndTime;
    @JsonProperty
    private long cachedAtTime;
    @JsonProperty
    private long erroredAtTime;
    @JsonProperty
    private long factCreatedAtTime;
    @JsonProperty
    private Map<String, String[]> params;


    public static class Builder {
        private QueryResultMeta instance;

        public Builder() {
            instance = new QueryResultMeta();
        }

        public Builder(QueryResultMeta queryResultMeta) {
            instance = queryResultMeta;
        }

        public Builder setThreadId(String threadId) {
            instance.threadId = threadId;
            return this;
        }

        public Builder setHostName(String hostName) {
            instance.hostName = hostName;
            return this;
        }

        public Builder setErrorMessage (String errorMessage) {
            instance.errorMessage = errorMessage;
            return this;
        }

        public Builder setStatus(QueryStatus status) {
            instance.status = status;
            return this;
        }

        public Builder setCached (boolean cached) {
            instance.cached = cached;
            return this;
        }

        public Builder setSubmittedAtTime(long submittedAtTime) {
            instance.submittedAtTime = submittedAtTime;
            return this;
        }

        public Builder setExecutionStartTime(long executionStartTime) {
            instance.executionStartTime = executionStartTime;
            return this;
        }

        public Builder setExecutionEndTime(long executionEndTime) {
            instance.executionEndTime = executionEndTime;
            return this;
        }

        public Builder setProcessingStartTime(long processingStartTime) {
            instance.processingStartTime = processingStartTime;
            return this;
        }

        public Builder setProcessingEndTime(long processingEndTime) {
            instance.processingEndTime = processingEndTime;
            return this;
        }

        public Builder setCachedAtTime (long cachedAtTime) {
            instance.cachedAtTime = cachedAtTime;
            return this;
        }

        public Builder setErroredAtTime (long erroredAtTime) {
            instance.erroredAtTime = erroredAtTime;
            return this;
        }

        public Builder setFactCreatedAtTime (long factCreatedAtTime) {
            instance.factCreatedAtTime = factCreatedAtTime;
            return this;
        }

        public Builder setParams (Map<String, String[]> params) {
            instance.params = params;
            return this;
        }

        public QueryResultMeta build() { return this.instance; }
    }

    public String getThreadId() {
        return threadId;
    }

    public String getHostName() {
        return hostName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public QueryStatus getStatus() {
        return status;
    }

    public boolean getCached() {
        return cached;
    }

    public long getSubmittedAtTime() { return submittedAtTime; }

    public long getExecutionStartTime() {
        return executionStartTime;
    }

    public long getExecutionEndTime() {
        return executionEndTime;
    }

    public long getProcessingStartTime() {
        return processingStartTime;
    }

    public long getProcessingEndTime() {
        return processingEndTime;
    }

    public long getCachedAtTime() {
        return cachedAtTime;
    }

    public long getErroredAtTime() {
        return erroredAtTime;
    }

    public long getFactCreatedAtTime() { return factCreatedAtTime; }

    public Map<String, String[]> getParams() { return params; }
}
