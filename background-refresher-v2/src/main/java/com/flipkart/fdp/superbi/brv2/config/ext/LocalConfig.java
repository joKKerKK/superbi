package com.flipkart.fdp.superbi.brv2.config.ext;

import com.beust.jcommander.internal.Lists;
import com.flipkart.fdp.compito.api.clients.consumer.ConsumerRatioConfig;
import com.flipkart.fdp.superbi.brv2.config.ApplicationConfig;
import com.flipkart.fdp.superbi.brv2.config.CircuitBreakerProperties;
import com.flipkart.fdp.superbi.brv2.config.DataSource;
import com.flipkart.kloud.config.error.ConfigServiceException;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.Delegate;

/**
 * Created by akshaya.sharma on 29/07/19
 */
public class LocalConfig implements ApplicationConfig {
  private final Map<String, List<String>> topicClusterMap = Maps.newHashMap();

  @Delegate(excludes = ExcludedListMethods.class)
  private final ApplicationConfig config;

  public LocalConfig(ApplicationConfig config) throws IOException, ConfigServiceException {
    this.config = config;
  }

  @Override
  public Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    return Maps.newHashMap();
  }

  @Override
  public ConsumerRatioConfig getConsumerRatioConfig(String storeIdentifier) {
    if(!topicClusterMap.containsKey(storeIdentifier + "-default")) {
      topicClusterMap.put(storeIdentifier + "-default", Lists.newArrayList("cluster-1"));
      topicClusterMap.put(storeIdentifier + "-retry", Lists.newArrayList("cluster-1"));
    }
    return new ConsumerRatioConfig(storeIdentifier, "cluster-1",1, Lists.newArrayList(
        new ConsumerRatioConfig("default", "cluster-1",8, null),
        new ConsumerRatioConfig("retry","cluster-1", 2, null)
    ));
  }

  @Override
  public Map<String, Map<String, Object>> getKafkaConsumerConfig() {
    Map<String, Map<String, Object>> kafkaConsumerConfig = Maps.newHashMap();
    kafkaConsumerConfig.putAll(config.getKafkaConsumerConfig());

    kafkaConsumerConfig.get("cluster-1")
        .put("bootstrap.servers", "host.docker.internal:29092");

    return kafkaConsumerConfig;
  }

  @Override
  public Map<String, Object> getKafkaProducerConfig() {
    Map<String, Object> kafkaProducerConfig = Maps.newHashMap();
    kafkaProducerConfig.putAll(config.getKafkaProducerConfig());

    kafkaProducerConfig.put("bootstrap.servers", "host.docker.internal:29092");

    return kafkaProducerConfig;
  }

  @Override
  public Map<String, List<String>> getTopicClusterMap() {
    return topicClusterMap;
  }

  @Override
  public Map<String, DataSource> getDataSourceMap() {
    Map<String, DataSource> map = new HashMap<>();
    DataSource localDataSource = new DataSource();
    localDataSource.setStoreIdentifier("MySQL_LOCAL");
    localDataSource.setSourceType("VERTICA");
    localDataSource.setDslConfig(Maps.newConcurrentMap());
    Map<String, Object> attributes = Maps.newHashMap();
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

  private abstract class ExcludedListMethods {
    public abstract Map<String, String> getPersistenceOverrides(String persistenceUnit);
    public abstract Map<String, DataSource> getDataSourceMap();
    public abstract Map<String,Map<String, Object>> getKafkaConsumerConfig();
    public abstract ConsumerRatioConfig getConsumerRatioConfig(String storeIdentifier);
    public abstract Map<String, List<String>> getTopicClusterMap();
    public abstract Map<String, Object> getKafkaProducerConfig();
  }
}
