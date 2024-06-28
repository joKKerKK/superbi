package com.flipkart.fdp.superbi.web.exception;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.core.exception.MalformedQueryException;
import com.flipkart.fdp.superbi.core.exception.SuperBiRuntimeException;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.web.filter.RequestIdFilter;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

@AllArgsConstructor
public class HttpErrorBody {
    private final ExceptionInfo info;
    @Getter
    private final Throwable exception;

    @JsonProperty("statusCode")
    public int getStatusCode() {
        return info.getStatusCode();
    }

    @JsonProperty("errorCode")
    public String getErrorCode() {
        if (StringUtils.isNotBlank(info.getErrorCode())) {
            return info.getErrorCode();
        }
        // TODO
        return "S-" + info.getStatusCode();
    }

    @JsonProperty("description")
    public String getDescription() {
        if (info.isAllowMessageForward() && exception != null) {
            return exception.getMessage();
        }
        return getErrorMessage();
    }

    @JsonProperty("message")
    public String getErrorMessage() {
        if (StringUtils.isNotBlank(info.getErrorMessage())) {
            return info.getErrorMessage();
        }
        if (exception instanceof MalformedQueryException){
            return exception.getMessage();
        }
        return "Something bad happened";
    }

    @JsonProperty("requestId")
    public String getRequestId() {
        return MDC.get(RequestIdFilter.REQUEST_ID);
    }

    @JsonProperty("queryCachedResult")
    public Optional<QueryResultCachedValue> getQueryResultCachedValue() {
        if( exception instanceof SuperBiRuntimeException)
            return ((SuperBiRuntimeException) exception).getCachedData();
        else return Optional.empty();
    }
}
