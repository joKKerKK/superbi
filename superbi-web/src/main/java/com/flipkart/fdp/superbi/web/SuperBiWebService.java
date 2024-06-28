package com.flipkart.fdp.superbi.web;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.utils.ObjectMapperUtils;
import com.flipkart.fdp.superbi.web.configurations.SuperBiWebServiceConfiguration;
import com.flipkart.fdp.superbi.web.exception.SuperBiExceptionMapper;
import com.flipkart.fdp.superbi.web.filter.RequestIdFilter;
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
import lombok.extern.slf4j.Slf4j;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by : waghmode.tayappa Date : Jun 20, 2019
 */
@Slf4j
public class SuperBiWebService extends Application<SuperBiWebServiceConfiguration> {

  private final SuperBiWebJerseyResourceProvider jerseyResourceProvider;
  private final SuperBiWebModuleProvider moduleProvider;
  protected final List<Module> guiceModules = Lists.newArrayList();

  private SuperBiWebService() {
    this.jerseyResourceProvider = new SuperBiWebJerseyResourceProvider();
    this.moduleProvider = new SuperBiWebModuleProvider();
  }

  public static void main(final String[] args) throws Exception {
//    new JHades().overlappingJarsReport();
    System.setProperty("org.jboss.logging.provider", "slf4j");
    new SuperBiWebService().run(args);
  }


  public void run(SuperBiWebServiceConfiguration serviceConfiguration, Environment environment) {
    guiceModules.addAll(moduleProvider.getModules(serviceConfiguration));
    final Injector injector = Guice.createInjector(guiceModules);
    for (Class k : jerseyResourceProvider.getJerseyResources()) {
      environment.jersey().register(injector.getInstance(k));
    }

    environment.jersey().register(new JsonProcessingExceptionMapper(true));
    ObjectMapperUtils.configure(environment.getObjectMapper());

    //Start Hystrix Metrics Publisher
    HystrixPlugins.getInstance().registerMetricsPublisher(
        HystrixServoMetricsPublisher.getInstance());


    // Register ExceptionMapper
    environment.jersey().register(injector.getInstance(SuperBiExceptionMapper.class));

    environment.servlets().addFilter("RequestIdFilter", new RequestIdFilter())
        .addMappingForUrlPatterns(
            EnumSet.allOf(DispatcherType.class), true, "/*");

    // Inject Auditor instance to cosmos components
    MetaCreator.get().setAuditer(injector.getInstance(Auditer.class));
  }

  public void initialize(Bootstrap<SuperBiWebServiceConfiguration> bootstrap) {

    bootstrap.addBundle(new SwaggerBundle<SuperBiWebServiceConfiguration>() {
      @Override
      public SwaggerBundleConfiguration getSwaggerBundleConfiguration(
          SuperBiWebServiceConfiguration configuration) {
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
