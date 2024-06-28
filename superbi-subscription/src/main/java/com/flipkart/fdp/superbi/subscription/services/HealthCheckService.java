package com.flipkart.fdp.superbi.subscription.services;

import com.flipkart.fdp.superbi.subscription.configurations.HealthCheckConfig;
import com.google.inject.Inject;
import java.io.File;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import org.quartz.Scheduler;

public class HealthCheckService {

  private final HealthCheckConfig healthCheckConfig;
  private final Scheduler scheduler;

  @Inject
  public HealthCheckService(HealthCheckConfig healthCheckConfig,Scheduler scheduler){
    this.healthCheckConfig = healthCheckConfig;
    this.scheduler = scheduler;
  }

  @SneakyThrows
  public HealthCheckResponse checkAll() {

    boolean isOOR = checkIfOOR();
    String message = isOOR ? "Service is OOR" : "Service is Up";
    if(isOOR && !scheduler.isInStandbyMode()){
      scheduler.standby();
    }else if(!isOOR && scheduler.isInStandbyMode()){
      scheduler.start();
    }
    return HealthCheckResponse.builder().passed(!isOOR).message(message).build();
  }

  private boolean checkIfOOR() {
    return (new File(this.healthCheckConfig.getOorFlagFilePath())).exists();
  }
  @Builder
  @Getter
  public static class HealthCheckResponse {
    boolean passed;
    String message;
  }
}