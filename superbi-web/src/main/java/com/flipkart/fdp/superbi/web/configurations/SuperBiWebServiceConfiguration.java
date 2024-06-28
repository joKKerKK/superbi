package com.flipkart.fdp.superbi.web.configurations;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.Getter;

/**
 * Created by : waghmode.tayappa
 * Date : Jun 20, 2019
 */
@Getter
public class SuperBiWebServiceConfiguration extends Configuration {

  private EnvironmentConfig environmentConfig;

  private HealthCheckConfig healthCheckConfig;

  @JsonProperty("swagger")
  public SwaggerBundleConfiguration swaggerBundleConfiguration;
}
