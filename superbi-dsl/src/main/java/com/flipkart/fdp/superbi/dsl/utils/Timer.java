package com.flipkart.fdp.superbi.dsl.utils;

/**
 * Created by amruth.s on 25/09/14.
 */

public class Timer {

    private long startTimeStampMs;
    private long endTimeStampMs;

    boolean stopped;
    boolean started;

    public Timer start() {
        if(started && stopped || !(started || stopped)) {
            startTimeStampMs = System.currentTimeMillis();
            endTimeStampMs = 0;
            stopped = false;
            started = true;
        }
        return this;
    }

    public Timer stop() {
        if(started && !stopped) {
            endTimeStampMs = System.currentTimeMillis();
            stopped = true;
        }
        return this;
    }

    public long getStartTimeMs() {
        return startTimeStampMs;
    }

    public long getEndTimeStampMs() {
        return endTimeStampMs;
    }

    public long getTimeTakenMs() {
        return endTimeStampMs - startTimeStampMs;
    }

    /**
     * @return elapsed time since started
     */
    public long getElapsedTimeTakenMs() {
        if (!started)
            throw new RuntimeException("Cannot stop timer without starting");
        return System.currentTimeMillis() - startTimeStampMs;
    }
}
