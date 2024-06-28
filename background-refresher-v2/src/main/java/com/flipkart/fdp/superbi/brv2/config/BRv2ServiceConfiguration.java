package com.flipkart.fdp.superbi.brv2.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.Getter;

/**
 * Created by : waghmode.tayappa
 * Date : Jun 20, 2019
 */
@Getter
public class BRv2ServiceConfiguration extends Configuration {

  private EnvironmentConfig environmentConfig;

  private HealthCheckConfig healthCheckConfig;

  @JsonProperty("swagger")
  public SwaggerBundleConfiguration swaggerBundleConfiguration;
}
