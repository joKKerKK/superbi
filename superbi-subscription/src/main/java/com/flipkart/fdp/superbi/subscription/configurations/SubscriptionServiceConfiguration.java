package com.flipkart.fdp.superbi.subscription.configurations;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import lombok.Getter;

@Getter
public class SubscriptionServiceConfiguration extends Configuration {

  private EnvironmentConfig environmentConfig;
  private HealthCheckConfig healthCheckConfig;
  @JsonProperty("swagger")
  public SwaggerBundleConfiguration swaggerBundleConfiguration;


}
