package com.flipkart.fdp.superbi.core.config;

import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsConfiguration;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankConfiguration;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClientConfiguration;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableClient;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.dao.validation.BatchCubeGuardrailConfig;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by akshaya.sharma on 18/06/19
 */

public interface SuperbiConfig {

  CacheExpiryConfig getCacheExpiryConfig(String storeIdentifier);

  BatchCubeGuardrailConfig getBatchCubeGuardrailConfig();

  Boolean isSourceRunningInSafeMode(String storeIdentifier);

  ClientPrivilege getClientPrivilege(String clientId);

//  using bigtable in gcp
//  CouchbaseBucketConfig getResultStoreBucketConfig();

  BigTableClient getResultTableClient() throws IOException;

  BigTableClient getLockTableClient() throws IOException;

//  using bigtable in gcp
//  CouchbaseBucketConfig getLockStoreBucketConfig();

  BackgroundRefresherConfig getBackgroundRefresherConfig(String storeIdentifier);

  GringottsConfiguration getGringottsConfiguration();

  IronBankConfiguration getIronBankConfiguration();

  MmgClientConfiguration getMmgClientConfiguration();

  Long getElasticSearchCostBoost(ESQuery.QueryType queryType);

  List<String> getDataSourcesList();

  Map<String, Map<String,String>> getDataSourceAttributes();

  Map<String, List<String>> getStoreIdentifiersForTableEnrich();

  Map<String,String> getTopicToInfraConfigRefMap();

  Map<String, Map<String, Object>> getPubsubLiteInfraConfigMap();

  MessageQueue getMessageQueue();

  default Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    return Maps.newHashMap();
  }

  default int getConcurrencyForScriptEngine() {
    return 4;
  }

  default int getDBAuditerMaxThreads() {
    return 100;
  }

  default int getDBAuditerRequestTimeout() {
    // 10s
    return 10000;
  }

  default boolean isDBAuditerEnabled() {
    return true;
  }

  default boolean isSuperBiAuditerEnabled() {
    return true;
  }

  default boolean checkDSQuerySerialization(String storeIdentifier) {
    return false;
  }

  default boolean shouldCalculatePriority(String storeIdentifier) {
    return false;
  }
}