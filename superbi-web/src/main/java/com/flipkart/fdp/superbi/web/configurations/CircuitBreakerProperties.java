package com.flipkart.fdp.superbi.web.configurations;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.RateLimitServerSideException;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadConfig.Builder;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import java.io.Serializable;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by akshaya.sharma on 31/12/19
 */

@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CircuitBreakerProperties implements Serializable {
  private Float failureRateThreshold;
  private Integer permittedNumberOfCallsInHalfOpenState;
  private Integer slidingWindowSize;
  private Integer minimumNumberOfCalls;
  private Long waitDurationInOpenState;
  private Boolean automaticTransitionFromOpenToHalfOpenEnabled;
  private Float slowCallRateThreshold;
  private Long slowCallDurationThreshold;
  private SlidingWindowType slidingWindowType;
  private Integer maxConcurrentCalls;
  private Long maxWaitDuration;

  public CircuitBreakerConfig getCircuitBreakerConfig() {
    CircuitBreakerConfig.Builder circuitBreakerBuilder = CircuitBreakerConfig.custom();

    circuitBreakerBuilder.ignoreExceptions(ClientSideException.class,
        RateLimitServerSideException.class);

    if(failureRateThreshold != null) {
      circuitBreakerBuilder.failureRateThreshold(failureRateThreshold);
    }

    if(slowCallRateThreshold != null) {
      circuitBreakerBuilder.slowCallRateThreshold(slowCallRateThreshold);
    }

    if(slowCallDurationThreshold != null) {
      circuitBreakerBuilder.slowCallDurationThreshold(Duration.ofMillis(slowCallDurationThreshold));
    }

    if(permittedNumberOfCallsInHalfOpenState != null) {
      circuitBreakerBuilder.permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState);
    }

    if(slidingWindowType != null) {
      circuitBreakerBuilder.slidingWindowType(slidingWindowType);
    }

    if(slidingWindowSize != null) {
      circuitBreakerBuilder.slidingWindowSize(slidingWindowSize);
    }

    if(minimumNumberOfCalls != null) {
      circuitBreakerBuilder.minimumNumberOfCalls(minimumNumberOfCalls);
    }

    if(automaticTransitionFromOpenToHalfOpenEnabled != null) {
      circuitBreakerBuilder.automaticTransitionFromOpenToHalfOpenEnabled(automaticTransitionFromOpenToHalfOpenEnabled);
    }

    if(waitDurationInOpenState != null) {
      circuitBreakerBuilder.waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenState));
    }

    return circuitBreakerBuilder.build();
  }

  public BulkheadConfig getBulkheadConfig() {
    Builder bulkheadBuilder = BulkheadConfig.custom();

    if(maxConcurrentCalls != null) {
      bulkheadBuilder.maxConcurrentCalls(maxConcurrentCalls);
    }

    if(maxWaitDuration != null) {
      bulkheadBuilder.maxWaitDuration(Duration.ofMillis(maxWaitDuration));
    }

    return bulkheadBuilder.build();
  }

}
