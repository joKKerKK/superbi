package com.flipkart.fdp.superbi.web.services;

import com.flipkart.fdp.superbi.web.configurations.HealthCheckConfig;
import com.google.inject.Inject;
import java.io.File;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by : waghmode.tayappa
 * Date : Jun 20, 2019
 */
public class HealthCheckService {

  private  HealthCheckConfig config;
  @Inject
  public HealthCheckService(HealthCheckConfig healthCheckConfig) {
    this.config = healthCheckConfig;
  }

  public HealthCheckResponse checkAll() {

    boolean isOOR = checkIfOOR();
    String message = isOOR ? "Service is OOR" : "Service is Up";
    return HealthCheckResponse.builder().passed(!isOOR).message(message).build();
  }

  private boolean checkIfOOR() {
    return (new File(this.config.getOorFlagFilePath())).exists();
  }
  @Builder
  @Getter
  public static class HealthCheckResponse {
    boolean passed;
    String message;
  }
}
