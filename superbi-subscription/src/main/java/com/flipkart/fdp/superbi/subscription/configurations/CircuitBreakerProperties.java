package com.flipkart.fdp.superbi.subscription.configurations;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadConfig.Builder;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import lombok.Setter;

/**
 * Created by akshaya.sharma on 31/12/19
 */

@Setter
public class CircuitBreakerProperties implements Serializable {
  private Optional<Float> failureRateThreshold;
  private Optional<Integer> permittedNumberOfCallsInHalfOpenState;
  private Optional<Integer> slidingWindowSize;
  private Optional<Integer> minimumNumberOfCalls;
  private Optional<Long> waitDurationInOpenState;
  private Optional<Boolean> automaticTransitionFromOpenToHalfOpenEnabled;
  private Optional<Float> slowCallRateThreshold;
  private Optional<Long> slowCallDurationThreshold;
  private Optional<SlidingWindowType> slidingWindowType;
  private Optional<Integer> maxConcurrentCalls;
  private Optional<Long> maxWaitDuration;

  @JsonCreator
  @lombok.Builder
  public CircuitBreakerProperties(@JsonProperty("failureRateThreshold") Optional<Float>  failureRateThreshold,
      @JsonProperty("permittedNumberOfCallsInHalfOpenState") Optional<Integer> permittedNumberOfCallsInHalfOpenState,
      @JsonProperty("slidingWindowSize") Optional<Integer> slidingWindowSize,
      @JsonProperty("minimumNumberOfCalls") Optional<Integer> minimumNumberOfCalls, @JsonProperty("waitDurationInOpenState") Optional<Long> waitDurationInOpenState,
      @JsonProperty("automaticTransitionFromOpenToHalfOpenEnabled") Optional<Boolean> automaticTransitionFromOpenToHalfOpenEnabled,
      @JsonProperty("slowCallRateThreshold") Optional<Float> slowCallRateThreshold,
      @JsonProperty("slowCallDurationThreshold") Optional<Long> slowCallDurationThreshold,
      @JsonProperty("slidingWindowType") Optional<SlidingWindowType> slidingWindowType, @JsonProperty("maxConcurrentCalls") Optional<Integer> maxConcurrentCalls,
      @JsonProperty("maxWaitDuration") Optional<Long> maxWaitDuration) {
    this.failureRateThreshold = failureRateThreshold;
    this.permittedNumberOfCallsInHalfOpenState = permittedNumberOfCallsInHalfOpenState;
    this.slidingWindowSize = slidingWindowSize;
    this.minimumNumberOfCalls = minimumNumberOfCalls;
    this.waitDurationInOpenState = waitDurationInOpenState;
    this.automaticTransitionFromOpenToHalfOpenEnabled = automaticTransitionFromOpenToHalfOpenEnabled;
    this.slowCallRateThreshold = slowCallRateThreshold;
    this.slowCallDurationThreshold = slowCallDurationThreshold;
    this.slidingWindowType = slidingWindowType;
    this.maxConcurrentCalls = maxConcurrentCalls;
    this.maxWaitDuration = maxWaitDuration;
  }

  public CircuitBreakerConfig getCircuitBreakerConfig() {
    CircuitBreakerConfig.Builder circuitBreakerBuilder = CircuitBreakerConfig.custom();
    circuitBreakerBuilder.ignoreExceptions(ClientSideException.class);

    failureRateThreshold.ifPresent(circuitBreakerBuilder::failureRateThreshold);

    slowCallRateThreshold.ifPresent(circuitBreakerBuilder::slowCallRateThreshold);
    slowCallDurationThreshold.ifPresent(threshold -> circuitBreakerBuilder.slowCallDurationThreshold(Duration.ofMillis(threshold)));

    permittedNumberOfCallsInHalfOpenState.ifPresent(circuitBreakerBuilder::permittedNumberOfCallsInHalfOpenState);
    slidingWindowType.ifPresent(circuitBreakerBuilder::slidingWindowType);
    slidingWindowSize.ifPresent(circuitBreakerBuilder::slidingWindowSize);

    minimumNumberOfCalls.ifPresent(circuitBreakerBuilder::minimumNumberOfCalls);
    automaticTransitionFromOpenToHalfOpenEnabled.ifPresent(circuitBreakerBuilder::automaticTransitionFromOpenToHalfOpenEnabled);

    waitDurationInOpenState.ifPresent(waitDurationInOpenState -> circuitBreakerBuilder.waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenState)));

    return circuitBreakerBuilder.build();
  }

  public BulkheadConfig getBulkheadConfig() {
    Builder bulkheadBuilder = BulkheadConfig.custom();

    maxConcurrentCalls.ifPresent(bulkheadBuilder::maxConcurrentCalls);
    maxWaitDuration.ifPresent(maxWaitDuration-> bulkheadBuilder.maxWaitDuration(Duration.ofMillis(maxWaitDuration)));

    return bulkheadBuilder.build();
  }

}
