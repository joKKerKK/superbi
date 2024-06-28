package com.flipkart.fdp.superbi;

import com.flipkart.fdp.superbi.exceptions.RateLimitServerSideException;
import com.flipkart.resilienthttpclient.exceptions.ClientSideException;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadConfig.Builder;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.retry.RetryConfig;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
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
  private Integer maxAttempts;
  private List<String> retryExceptions;
  private List<String> ignoreExceptions;

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

  @SneakyThrows
  public RetryConfig getRetryConfig(){
    RetryConfig.Builder retryConfigBuilder = RetryConfig.custom();
    if(!(retryExceptions == null || retryExceptions.isEmpty())){
      Class[] retryExceptionsArray = this.retryExceptions.stream()
          .map(i -> {
            try {
              return Class.forName(i);
            } catch (ClassNotFoundException e) {
              return null;
            }
          })
          .toArray(Class[]::new);
      retryConfigBuilder.retryExceptions(retryExceptionsArray);
    }
    if(!(ignoreExceptions == null || ignoreExceptions.isEmpty())){
      Class[] retryExceptionsArray = this.ignoreExceptions.stream()
          .map(i-> { try { return Class.forName(i); } catch (ClassNotFoundException e) {return  null;}})
          .toArray(Class[] ::new);
      retryConfigBuilder.retryExceptions(retryExceptionsArray);
    }
    if(maxAttempts != null){
      retryConfigBuilder.maxAttempts(maxAttempts);
    }
    return retryConfigBuilder.build();
  }
}

