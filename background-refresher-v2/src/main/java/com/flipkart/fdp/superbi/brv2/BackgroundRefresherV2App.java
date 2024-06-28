package com.flipkart.fdp.superbi.brv2;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.compito.core.CompitoEngine;
import com.flipkart.fdp.superbi.brv2.config.BRv2ServiceConfiguration;
import com.flipkart.fdp.superbi.brv2.resource.HealthCheck;
import com.flipkart.fdp.superbi.utils.ObjectMapperUtils;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.hystrix.contrib.servopublisher.HystrixServoMetricsPublisher;
import com.netflix.hystrix.strategy.HystrixPlugins;
import io.dropwizard.Application;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by : waghmode.tayappa Date : Jun 20, 2019
 */
@Slf4j
public class BackgroundRefresherV2App extends Application<BRv2ServiceConfiguration> {

  private final List<Module> guiceModules = Lists.newArrayList();


  public static void main(final String[] args) throws Exception {
    System.setProperty("org.jboss.logging.provider", "slf4j");
    new BackgroundRefresherV2App().run(args);
  }

  public void run(BRv2ServiceConfiguration serviceConfiguration, Environment environment) {
    guiceModules.add(new BackgroundRefresherV2Module(serviceConfiguration));
    final Injector injector = Guice.createInjector(guiceModules);
    environment.jersey().register(injector.getInstance(HealthCheck.class));

    environment.jersey().register(new JsonProcessingExceptionMapper(true));
    ObjectMapperUtils.configure(environment.getObjectMapper());
    CompitoEngine compitoEngine = injector.getInstance(CompitoEngine.class);
    //Start Hystrix Metrics Publisher
    HystrixPlugins.getInstance().registerMetricsPublisher(
        HystrixServoMetricsPublisher.getInstance());
    compitoEngine.start();
  }

  public void initialize(Bootstrap<BRv2ServiceConfiguration> bootstrap) {
    bootstrap.addBundle(new SwaggerBundle<BRv2ServiceConfiguration>() {
      @Override
      public SwaggerBundleConfiguration getSwaggerBundleConfiguration(
          BRv2ServiceConfiguration configuration) {
        return configuration.swaggerBundleConfiguration;
      }
    });

    guiceModules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricRegistry.class).toInstance(bootstrap.getMetricRegistry());
        bind(ObjectMapper.class).toInstance(bootstrap.getObjectMapper());
        bind(HealthCheckRegistry.class).toInstance(bootstrap.getHealthCheckRegistry());
      }
    });
  }
}
