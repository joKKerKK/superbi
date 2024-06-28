package com.flipkart.fdp.superbi.brv2.config;

import com.flipkart.fdp.compito.api.retry.RetryConfig;
import com.flipkart.fdp.compito.api.clients.consumer.ConsumerRatioConfig;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableClient;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.couchbase.CouchbaseBucketConfig;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by akshaya.sharma on 18/06/19
 */

public interface BRv2Config {
//  CouchbaseBucketConfig getResultStoreBucketConfig();

  BigTableClient getResultTableClient() throws IOException;

  BackgroundRefresherConfig getBackgroundRefresherConfig(String storeIdentifier);

  default int getConcurrencyForScriptEngine() {
    return 4;
  }

  default int getDBAuditorMaxThreads() {
    return 100;
  }

  default int getDBAuditorRequestTimeout() {
    // 10s
    return 10000;
  }

  default boolean isDBAuditorEnabled() {
    return true;
  }

  default boolean isAuditorEnabled() {
    return true;
  }

  default Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    return Maps.newHashMap();
  }

  ConsumerRatioConfig getConsumerRatioConfig(String storeIdentifier);

  DedupeStoreConfig getDedupeStoreConfig();

  long getDedupeStoreTTLInMs(String storeIdentifier);

  long getTTLAfterSuccessInMillis(String storeIdentifier);

  Map<String, Object> getKafkaProducerConfig();

  Map<String, Map<String, Object>> getKafkaConsumerConfig();

  Map<String,List<String>> getTopicClusterMap();

  long getMaxCapacity(String storeIdentifier);

  RetryConfig getRetryConfig(String storeIdentifier);

  Map<String,String> getTopicToInfraConfigRefMap();

  Map<String, Map<String, Object>> getPubsubLiteInfraConfigMap();

  Map<String,List<String>> getSubscriptionConfigRefMap();

  Map<String,List<String>> getPrimarySecondarySubscriptionMap();

  MessageQueue getMessageQueue();
}