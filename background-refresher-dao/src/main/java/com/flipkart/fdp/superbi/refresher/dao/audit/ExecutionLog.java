package com.flipkart.fdp.superbi.refresher.dao.audit;


import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ExecutionLog {

    private String id;

    private String sourceName;

    private String dsQuery;

    private String translatedQuery;

    private long startTimeStampMs;

    private long translationTimeMs;

    private long executionTimeMs;

    private long totalTimeMs;

    private boolean isCompleted;

    private boolean isSlowQuery;

    private boolean cacheHit;

    private String message;

    private int attemptNumber;

    private String requestId;

    private String factName;
}
