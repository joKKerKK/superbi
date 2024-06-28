package com.flipkart.fdp.superbi.cosmos.meta.db.health;

/**
 * User: aniruddha.gangopadhyay
 * Date: 27/02/14
 * Time: 10:14 PM
 */
public interface HealthStatus {
    boolean isHealthy();
    String message();
}