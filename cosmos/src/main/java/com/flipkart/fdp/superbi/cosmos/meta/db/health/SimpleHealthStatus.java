package com.flipkart.fdp.superbi.cosmos.meta.db.health;

/**
 * User: aniruddha.gangopadhyay
 * Date: 27/02/14
 * Time: 10:14 PM
 */
public class SimpleHealthStatus implements HealthStatus {

    private final boolean healthy;
    private final String message;

    public SimpleHealthStatus(boolean healthy, String message) {
        this.healthy = healthy;
        this.message = message;
    }

    @Override
    public boolean isHealthy() {
        return healthy;
    }

    @Override
    public String message() {
        return message;
    }

    @Override
    public String toString() {
        return "\t" + message + ": " + healthy;
    }
}
