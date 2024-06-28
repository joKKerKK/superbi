package com.flipkart.fdp.superbi.subscription;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.subscription.app.SubscriptionServiceModule;
import com.flipkart.fdp.superbi.subscription.configurations.SubscriptionServiceConfiguration;
import com.flipkart.fdp.superbi.subscription.resources.HealthCheck;
import com.flipkart.fdp.superbi.utils.ObjectMapperUtils;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import java.util.List;
import lombok.SneakyThrows;
import org.quartz.Scheduler;

public class SubscriptionService extends Application<SubscriptionServiceConfiguration> {

  protected final List<Module> guiceModules = Lists.newArrayList();

  public static void main(final String[] args) throws Exception {
    System.setProperty("org.jboss.logging.provider", "slf4j");
    new SubscriptionService().run(args);
  }

  @SneakyThrows
  public void run(SubscriptionServiceConfiguration serviceConfiguration, Environment environment) {
    guiceModules.add(new SubscriptionServiceModule(serviceConfiguration));
    final Injector injector = Guice.createInjector(guiceModules);

    environment.jersey().register(injector.getInstance(HealthCheck.class));
    environment.jersey().register(new JsonProcessingExceptionMapper(true));

    Scheduler scheduler = injector.getInstance(Scheduler.class);
    scheduler.start();
    ObjectMapperUtils.configure(environment.getObjectMapper());
  }

  public void initialize(Bootstrap<SubscriptionServiceConfiguration> bootstrap) {

    bootstrap.addBundle(new SwaggerBundle<SubscriptionServiceConfiguration>() {
      @Override
      public SwaggerBundleConfiguration getSwaggerBundleConfiguration(
          SubscriptionServiceConfiguration configuration) {
        return configuration.swaggerBundleConfiguration;
      }
    });

    guiceModules.add(new AbstractModule(){
      @Override
      protected void configure() {
        bind(MetricRegistry.class).toInstance(bootstrap.getMetricRegistry());
        bind(ObjectMapper.class).toInstance(bootstrap.getObjectMapper());
        bind(HealthCheckRegistry.class).toInstance(bootstrap.getHealthCheckRegistry());
      }
    });

  }
}
