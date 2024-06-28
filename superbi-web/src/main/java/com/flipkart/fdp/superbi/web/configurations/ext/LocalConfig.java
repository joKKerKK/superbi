package com.flipkart.fdp.superbi.web.configurations.ext;

import com.flipkart.fdp.superbi.web.configurations.ApplicationConfig;
import com.flipkart.fdp.superbi.web.configurations.CircuitBreakerProperties;
import com.flipkart.fdp.superbi.web.configurations.DataSource;
import com.google.common.collect.Maps;
import java.util.HashMap;
import lombok.experimental.Delegate;

import java.util.Map;

/**
 * Created by akshaya.sharma on 29/07/19
 */
public class LocalConfig implements ApplicationConfig {
  @Delegate(excludes = ExcludedListMethods.class)
  private final ApplicationConfig config;

  public LocalConfig(ApplicationConfig config) {
    this.config = config;
  }

  @Override
  public Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    return Maps.newHashMap();
  }

  @Override
  public Map<String, DataSource> getDataSourceMap() {
    Map<String, DataSource> map = new HashMap<>();
    DataSource localDataSource = new DataSource();
    localDataSource.setStoreIdentifier("MySQL_LOCAL");
    localDataSource.setSourceType("VERTICA");
    localDataSource.setDslConfig(Maps.newConcurrentMap());
    Map<String, String> attributes = Maps.newHashMap();
    attributes.put("jdbcUrl", "jdbc:mysql://host.docker.internal/cosmos");
    attributes.put("User", "remote");
    attributes.put("Password", "remote");

    attributes.put("user", "remote");
    attributes.put("password", "remote");

    localDataSource.setAttributes(attributes);
    localDataSource.setCircuitBreakerProperties(new CircuitBreakerProperties());

    map.put(localDataSource.getStoreIdentifier(), localDataSource);
    return map;
  }

  @Override
  public Map<String, Object> getKafkaProducerConfig() {
    Map<String, Object> kafkaProducerConfig = Maps.newHashMap();
    kafkaProducerConfig.putAll(config.getKafkaProducerConfig());

    kafkaProducerConfig.put("bootstrap.servers", "host.docker.internal:29092");

    return kafkaProducerConfig;
  }

  private abstract class ExcludedListMethods {
    public abstract Map<String, String> getPersistenceOverrides(String persistenceUnit);
    public abstract Map<String, DataSource> getDataSourceMap();
    public abstract Map<String, Object> getKafkaProducerConfig();
  }
}