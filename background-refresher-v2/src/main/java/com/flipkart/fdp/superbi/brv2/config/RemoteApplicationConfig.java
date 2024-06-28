
package com.flipkart.fdp.superbi.brv2.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.fdp.compito.api.retry.RetryConfig;
import com.flipkart.fdp.compito.api.clients.consumer.ConsumerRatioConfig;
import com.flipkart.fdp.config.ConfigBuilder;
import com.flipkart.fdp.config.ConfigProperty;
import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.gcs.GcsConfig;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsConfiguration;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankConfiguration;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableClient;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableConfig;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.config.D42MetaConfig;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClientConfiguration;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.fdp.superbi.utils.SuperbiUtil;
import com.flipkart.kloud.config.error.ConfigServiceException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Observable;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import rx.functions.Action3;

/**
 * Created by akshaya.sharma on 23/07/19
 */
@Slf4j
public class RemoteApplicationConfig extends Observable implements ApplicationConfig {

  private static final long DEFAULT_DEDUPESTORE_TTL = 60 * 60 * 1000;
  private static final long DEFAULT_MAX_CAPACITY = 100L;
  private static final long DEFAULT_TTL_AFTER_SUCCESS = 5 * 60 * 1000;

  private Gson gson = new Gson();
  @ConfigProperty("superbi.c3p0.maxStatementsMultiplier")
  private volatile int maxStatementsMultiplier = 5;
  @ConfigProperty("superbi.executorService.threadCount")
  private volatile int executorServiceThreadCount;
  @ConfigProperty("superbi.dataSources")
  private String dataSourcesStr;
  private List<String> dataSourcesList;
  private Map<String, DataSource> superbiStoreMap = new HashMap<>();
//  @ConfigProperty("cachestore.nodes")
//  @Sanitize
//  private volatile String couchbaseNodes = "";
//  @ConfigProperty("query_results.name")
//  @Sanitize
//  private volatile String resultBucket = "query_results";
//  @ConfigProperty("query_results.password")
//  @Sanitize
//  private volatile String resultsBucketPassword;
//  @ConfigProperty("query_cache.ObsTimeoutMS")
//  @Sanitize
//  private volatile String resultBucketObsTimeoutMS;
//  @ConfigProperty("query_cache.OpTimeoutMS")
//  @Sanitize
//  private volatile String resultBucketOpTimeoutMS;
//  @ConfigProperty("query_cache.OpQueueMaxBlockTimeMS")
//  @Sanitize
//  private volatile String resultBucketOpQueueMaxBlockTimeMS;
//  private CouchbaseBucketConfig resultStoreBucketConfig;

  @ConfigProperty("bigtable.config")
  private volatile String bigTableConfigStr;

  @ConfigProperty("query_results.tableId")
  @Sanitize
  private volatile String resultTable = "query_results";

  /**
   * Kafka Producer Config for BRv2.
   */
  @ConfigProperty("superbi.kafkaProducerConfig")
  private volatile String kafkaProducerConfigStr;
  private Map<String, Object> kafkaProducerConfig = Maps.newConcurrentMap();

  @ConfigProperty("superbi.ironbank")
  private volatile String ironBankStr;
  private IronBankConfiguration ironBankConfiguration;

  @ConfigProperty("superbi.druidClient")
  private volatile String druidClientStr;
  private DruidClientConfiguration druidClientConfiguration;

  @ConfigProperty("superbi.fStreamClient")
  private volatile String fStreamClientStr;
  private FStreamClientConfiguration fStreamClientConfiguration;

  @Override
  public D42MetaConfig getD42MetaConfig() {

    if(d42MetaConfig == null){
      log.error("Invalid D42MetaConfig");
      throw new RuntimeException("D42MetaConfig can't be null");
    }
    return d42MetaConfig;
  }

  @Override
  public DruidClientConfiguration getDruidClientConfiguration() {
    if (druidClientConfiguration == null) {
      log.error("Invalid Druid config");
      throw new RuntimeException("Invalid Druid config");
    }
    return druidClientConfiguration;
  }

  @Override
  public FStreamClientConfiguration getFStreamClientConfiguration() {
    if (fStreamClientConfiguration == null) {
      log.error("Invalid FStream configuration");
      throw new RuntimeException("Invalid FStream Configuration");
    }
    return fStreamClientConfiguration;
  }

  private BigtableDataClient getBigTableDataClient() throws IOException {
    return this.getBigTableConfig().getBigTableDataClient();
  }

  private BigTableConfig getBigTableConfig() {
    return gson.fromJson(this.bigTableConfigStr, BigTableConfig.class);
  }

  public String getResultTable(){
    return resultTable;
  }

  @SneakyThrows
  private void buildIronBankConfiguration() {
    this.ironBankConfiguration = gson.fromJson(ironBankStr, IronBankConfiguration.class);
  }

  @SneakyThrows
  private void buildDruidClientConfiguration() {
    this.druidClientConfiguration = gson.fromJson(druidClientStr, DruidClientConfiguration.class);
  }

  @SneakyThrows
  private void buildTopicClusterMap() {
    this.topicClusterMap = gson.fromJson(topicClusterMapStr,
        new TypeToken<ConcurrentHashMap<String, List<String>>>() {
        }.getType());
  }

  private void buildFStreamClientConfiguration() {
    this.fStreamClientConfiguration = gson.fromJson(fStreamClientStr, FStreamClientConfiguration.class);
  }

  @Override
  public IronBankConfiguration getIronBankConfiguration() {
    if (ironBankConfiguration == null) {
      log.error("Invalid IronBankConfiguration config");
      throw new RuntimeException("Invalid IronBankConfiguration config");
    }
    return ironBankConfiguration;
  }

  @ConfigProperty("superbi.gringotts")
  private volatile String gringottsStr;
  private GringottsConfiguration gringottsConfiguration;

  @SneakyThrows
  private void buildGringottsConfiguration() {
    this.gringottsConfiguration = gson.fromJson(gringottsStr, GringottsConfiguration.class);
  }

  @Override
  public GringottsConfiguration getGringottsConfiguration() {
    if (gringottsConfiguration == null) {
      log.error("Invalid Gringotts config");
      throw new RuntimeException("Invalid Gringotts config");
    }
    return gringottsConfiguration;
  }


  @SneakyThrows
  private void buildKafkaProducerConfig() {
    this.kafkaProducerConfig = gson.fromJson(kafkaProducerConfigStr,
        new TypeToken<ConcurrentHashMap<String, Object>>() {
        }.getType());
  }

  /**
   * Kafka Consumer Config for BRv2.
   */
  @ConfigProperty("brv2.kafkaConsumerConfig")
  private volatile String kafkaConsumerConfigStr;
  private Map<String, Map<String, Object>> kafkaConsumerConfig = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildKafkaConsumerConfig() {
    this.kafkaConsumerConfig = gson.fromJson(kafkaConsumerConfigStr,
        new TypeToken<ConcurrentHashMap<String, Map<String,Object>>>() {
        }.getType());
  }

  @ConfigProperty("superbi.topicClusterMap")
  private volatile String topicClusterMapStr;
  private Map<String,List<String>> topicClusterMap;


  /**
   * Consumer Ratio Config for BRv2.
   */
  @ConfigProperty("brv2.consumerRatioConfigs")
  private volatile String consumerRatioConfigStr;
  private Map<String, ConsumerRatioConfig> consumerRatioConfigMap = Maps.newConcurrentMap();

  /*
  add superbi.MessageQueue, superbi.topicToInfraConfigRefMap,
  brv2.pubsubLiteInfraConfig in both superbi & Brv2 config bucket
   */
  @ConfigProperty("superbi.messageQueue")
  private volatile String messageQueue;

  @ConfigProperty("superbi.subscriptionConfigRefMap")
  private volatile String subscriptionConfigRefStr;
  private Map<String,List<String>> subscriptionConfigRefMap;

  @SneakyThrows
  private void buildSubscriptionConfigRefMap() {
    this.subscriptionConfigRefMap = gson.fromJson(subscriptionConfigRefStr,
        new TypeToken<ConcurrentHashMap<String, List<String>>>() {
        }.getType());
  }

  @ConfigProperty("brv2.primarySecondarySubscriptionConfig")
  private volatile String primarySecondarySubscriptionConfigStr;
  private Map<String,List<String>> primarySecondarySubscriptionConfig = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildPrimarySecondaryConfig(){
    this.primarySecondarySubscriptionConfig = gson.fromJson(primarySecondarySubscriptionConfigStr,
        new TypeToken<ConcurrentHashMap<String, List<String>>>() {
        }.getType());
  }

  @ConfigProperty("superbi.topicToInfraConfigRefMap")
  private volatile String topicToInfraConfigRefStr;
  private Map<String,String> topicToInfraConfigRefMap;

  @SneakyThrows
  private void buildTopicConfigRefMap() {
    this.topicToInfraConfigRefMap = gson.fromJson(topicToInfraConfigRefStr,
        new TypeToken<ConcurrentHashMap<String, String>>() {
        }.getType());
  }

  @ConfigProperty("brv2.pubsubLiteInfraConfig")
  private volatile String pubsubLiteInfraConfigStr;
  private Map<String, Map<String, Object>> pubsubLiteInfraConfigMap = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildpubsubLiteInfraConfigMap() {
    this.pubsubLiteInfraConfigMap = gson.fromJson(pubsubLiteInfraConfigStr,
        new TypeToken<ConcurrentHashMap<String, Map<String,Object>>>() {
        }.getType());
  }

  /*
  DedupeStoreTTL for BRv2.
   */
  @ConfigProperty("brv2.dedupeStoreTTLInMs")
  private volatile String dedupeStoreTTLStr;
  private Map<String, Long> dedupeStoreTTLInMsMap = Maps.newConcurrentMap();

  /*
  DedupeStoreTTL for BRv2.
   */
  @ConfigProperty("brv2.ttlAfterSuccessInMillis")
  private volatile String ttlAfterSuccessInMillisStr;
  private Map<String, Long> ttlAfterSuccessInMillisMap = Maps.newConcurrentMap();


  /*
  MaxCapacity Per Source for BRv2.
   */
  @ConfigProperty("brv2.maxCapacityConfigs")
  private volatile String maxCapacityConfigsStr;
  private Map<String, Long> maxCapacityConfigsMap = Maps.newConcurrentMap();


  /*
  DedupeStoreTTL for BRv2.
   */
  @ConfigProperty("brv2.dedupeStoreConfig")
  private volatile String dedupeStoreConfigStr;
  private DedupeStoreConfig dedupeStoreConfig;

  /*
  DedupeStoreTTL for BRv2.
   */
  @ConfigProperty("brv2.retryConfigs")
  private volatile String retryConfigsStr;
  private Map<String, RetryConfig> retryConfigsMap;

  private RetryConfig DEFAULT_RETRY_CONFIG = new RetryConfig(Lists.newArrayList());


  /*
  BackgroundRefresherConfig
   */
  @ConfigProperty("superbi.backgroundRefresherConfigs")
  private volatile String backgroundRefresherConfigsStr;
  private Map<String, BackgroundRefresherConfig> backgroundRefresherConfigMap = Maps
      .newConcurrentMap();
  @ConfigProperty("superbi.defaultBackgroundRefresherConfig")
  private volatile String defaultBackgroundRefresherConfigStr;
  private BackgroundRefresherConfig defaultBackgroundRefresherConfig = new
      BackgroundRefresherConfig(
      500, 100000, 100, 2000, 200, 3600 * 24,
      2, 5000, 10 * 1024 * 1024L, 0.0d,1024);
  @ConfigProperty(("superbi.d42"))
  private volatile String d42ConfigStr;
  private D42Configuration d42Configuration;

  @ConfigProperty(("superbi.gcs"))
  private volatile String gcsConfigStr;
  private GcsConfig gcsConfig;
  @ConfigProperty("superbi.concurrencyForScriptEngine")
  private volatile int concurrencyForScriptEngine = 5;
  @ConfigProperty("AuditStoreDbLogger.enable")
  private volatile String enableDBAuditer = "false";
  @ConfigProperty("audit.enable")
  private volatile String enableAudit = "false";

  @ConfigProperty("superbi.d42MetaConfig")
  private String d42MetaConfigStr;
  private D42MetaConfig d42MetaConfig;


  private void buildD42MetaConfig(){
    this.d42MetaConfig = gson.fromJson(d42MetaConfigStr, D42MetaConfig.class);
  }

  @SneakyThrows
  public RemoteApplicationConfig() throws IOException, ConfigServiceException {
  }

  private static int getIntValue(String value, int defaultValue) {
    try {
      if (StringUtils.isBlank(value)) {
        return defaultValue;
      }
      return Integer.parseInt(value);
    } catch (Exception ex) {
      log.error("Could not convert to int {} {}", value, ex.getMessage());
    }
    return defaultValue;
  }

  private static String sanitizeString(String value) {
    if (StringUtils.isNotBlank(value)) {
      if (value.startsWith("\"") && value.endsWith("\"")) {
        return StringUtils.substring(value, 1, value.length() - 1);
      }
    }
    return value;
  }

  private static int getIntValue(String value) {
    return getIntValue(value, 0);
  }

  public void buildSuperBiStoreMap(ConfigBuilder configBuilder) {
    this.dataSourcesList.stream().forEach(i -> this.superbiStoreMap.put(String.valueOf(i),
        gson.fromJson(configBuilder.getKeyAsObject("superbi.dataSource.".concat(i), String.class),
            DataSource.class)));

  }

  private void buildValidStoreList() {
    this.dataSourcesList = gson.fromJson(this.dataSourcesStr, List.class);
  }

//  @SneakyThrows
//  private void buildResultStoreBucketConfig() {
//    this.resultStoreBucketConfig = new CouchbaseBucketConfig(
//        couchbaseNodes.split(","), resultBucket, resultsBucketPassword,
//        getIntValue(resultBucketObsTimeoutMS),
//        getIntValue(resultBucketOpTimeoutMS),
//        getIntValue(resultBucketOpQueueMaxBlockTimeMS)
//    );
//  }

  @SneakyThrows
  private void buildConsumerRatioConfigs() {
    this.consumerRatioConfigMap = gson.fromJson(consumerRatioConfigStr,
        new TypeToken<ConcurrentHashMap<String, ConsumerRatioConfig>>() {
        }.getType());
  }

  @SneakyThrows
  private void buildBackgroundRefresherConfigs() {
    this.backgroundRefresherConfigMap = gson.fromJson(backgroundRefresherConfigsStr,
        new TypeToken<ConcurrentHashMap<String, BackgroundRefresherConfig>>() {
        }.getType());
  }

  @SneakyThrows
  private void buildDefaultBackgroundRefresherConfig() {
    this.defaultBackgroundRefresherConfig = gson.fromJson(defaultBackgroundRefresherConfigStr,
        BackgroundRefresherConfig.class);
  }

  @SneakyThrows
  private void buildDedupeStoreTTLConfig() {
    this.dedupeStoreTTLInMsMap = gson.fromJson(dedupeStoreTTLStr,
        new TypeToken<ConcurrentHashMap<String, Long>>() {
        }.getType());
  }

  @SneakyThrows
  private void buildTTLAfterSuccessConfig() {
    this.ttlAfterSuccessInMillisMap = gson.fromJson(ttlAfterSuccessInMillisStr,
        new TypeToken<ConcurrentHashMap<String, Long>>() {
        }.getType());
  }


  @SneakyThrows
  private void buildRetryConfigs() {
    TypeReference<ConcurrentHashMap<String, RetryConfig>> typeRef = new TypeReference<ConcurrentHashMap<String, RetryConfig>>() {
    };
    this.retryConfigsMap = JsonUtil.fromJson(retryConfigsStr, typeRef);
  }


  @SneakyThrows
  private void buildMaxCapacityConfigs() {
    this.maxCapacityConfigsMap = gson.fromJson(maxCapacityConfigsStr,
        new TypeToken<ConcurrentHashMap<String, Long>>() {
        }.getType());
  }

  @SneakyThrows
  private void buildDedupeStoreConfig() {
    this.dedupeStoreConfig = gson.fromJson(dedupeStoreConfigStr, DedupeStoreConfig.class);
  }

//  @Override
//  public CouchbaseBucketConfig getResultStoreBucketConfig() {
//    return this.resultStoreBucketConfig;
//  }

  @Override
  public BigTableClient getResultTableClient() throws IOException {
    return new BigTableClient(getBigTableDataClient(), getResultTable());
  }

  @Override
  public BackgroundRefresherConfig getBackgroundRefresherConfig(String storeIdentifier) {
    if (StringUtils.isNotBlank(storeIdentifier) && backgroundRefresherConfigMap.containsKey(
        storeIdentifier)) {
      return backgroundRefresherConfigMap.get(storeIdentifier);
    }
    return defaultBackgroundRefresherConfig;
  }

  @Override
  public ConsumerRatioConfig getConsumerRatioConfig(String storeIdentifier) {
    if (StringUtils.isNotBlank(storeIdentifier) && consumerRatioConfigMap
        .containsKey(storeIdentifier)) {
      return consumerRatioConfigMap.get(storeIdentifier);
    }
    throw new RuntimeException("ConsumerRatioConfig is not defined for " + storeIdentifier);
  }

  @Override
  public DedupeStoreConfig getDedupeStoreConfig() {
    if (this.dedupeStoreConfig != null) {
      return this.dedupeStoreConfig;
    }
    //Default DedupeStoreConfig.
    return new DedupeStoreConfig(1000000, 60 * 24);
  }

  @Override
  public long getDedupeStoreTTLInMs(String storeIdentifier) {
    if (StringUtils.isNotBlank(storeIdentifier)) {
      return dedupeStoreTTLInMsMap.getOrDefault(storeIdentifier, DEFAULT_DEDUPESTORE_TTL);
    }
    return DEFAULT_DEDUPESTORE_TTL;
  }

  @Override
  public long getTTLAfterSuccessInMillis(String storeIdentifier) {
    if (ttlAfterSuccessInMillisMap != null && StringUtils.isNotBlank(storeIdentifier)) {
      return ttlAfterSuccessInMillisMap.getOrDefault(storeIdentifier, DEFAULT_TTL_AFTER_SUCCESS);
    }
    return DEFAULT_TTL_AFTER_SUCCESS;
  }

  @Override
  public Map<String, Object> getKafkaProducerConfig() {
    if (kafkaProducerConfig == null || kafkaProducerConfig.isEmpty()) {
      log.error("Invalid Kafka Producer config for BRv2.");
      throw new RuntimeException("Invalid Kafka Producer config for BRv2.");
    }
    return kafkaProducerConfig;
  }


  @Override
  public Map<String,Map<String, Object>> getKafkaConsumerConfig() {
    if (kafkaConsumerConfig == null || kafkaConsumerConfig.isEmpty()) {
      log.error("Invalid Kafka Consumer config for BRv2.");
      throw new RuntimeException("Invalid Kafka Consumer config for BRv2.");
    }
    return kafkaConsumerConfig;
  }

  @Override
  public Map<String, List<String>> getTopicClusterMap() {
    return topicClusterMap;
  }

  @Override
  public long getMaxCapacity(String storeIdentifier) {
    if (maxCapacityConfigsMap != null) {
      return maxCapacityConfigsMap.getOrDefault(storeIdentifier, DEFAULT_MAX_CAPACITY);
    }
    return DEFAULT_MAX_CAPACITY;
  }

  @Override
  public RetryConfig getRetryConfig(String storeIdentifier) {
    if (retryConfigsMap != null) {
      return retryConfigsMap.getOrDefault(storeIdentifier, DEFAULT_RETRY_CONFIG);
    }
    return DEFAULT_RETRY_CONFIG;
  }


  public void buildD42Configuration() {
    this.d42Configuration = gson.fromJson(this.d42ConfigStr, D42Configuration.class);
  }

  public void buildGcsConfiguration() {
    this.gcsConfig = gson.fromJson(this.gcsConfigStr, GcsConfig.class);
  }

  @Override
  public D42Configuration getD42Configuration() {
    return d42Configuration;
  }

  @Override
  public GcsConfig getGcsConfig() {
    return gcsConfig;
  }

  @Override
  public Map<String, DataSource> getDataSourceMap() {
    return superbiStoreMap;
  }

  @Override
  public int getExecutorServiceThreadCount() {
    return executorServiceThreadCount;
  }

  @Override
  public int getConcurrencyForScriptEngine() {
    return concurrencyForScriptEngine;
  }


  private Map<String, Map<String, String>> persistenceOverridesMap = Maps
      .newHashMap();

  private Map<String, String> mapHydraDbPropertiesToPersistence() {
    Map<String, String> keyMaps = new HashMap<>();
    String jdbcUrl = "database.jdbcUrl";
    keyMaps.put(jdbcUrl, "javax.persistence.jdbc.url");

    String acquireIncrement = "database.acquireIncrement";
    keyMaps.put(acquireIncrement, "hibernate.c3p0.acquireIncrement");

    String maxPoolSize = "database.maxPoolSize";
    keyMaps.put(maxPoolSize, "hibernate.c3p0.maxPoolSize");

    String acquireRetryDelay = "database.acquireRetryDelay";
    keyMaps.put(acquireRetryDelay, "hibernate.c3p0.acquireRetryDelay");

    String connectionTimeout = "database.connectionTimeout";
    keyMaps.put(connectionTimeout, "hibernate.c3p0.timeout");

    String password = "database.password";
    keyMaps.put(password, "javax.persistence.jdbc.password");

    String acquireRetryAttempt = "database.acquireRetryAttempt";
    keyMaps.put(acquireRetryAttempt, "hibernate.c3p0.acquireRetryAttempts");

    String hibernateShowSql = "database.hibernateShowSql";
    keyMaps.put(hibernateShowSql, "hibernate.show_sql");

    String userName = "database.userName";
    keyMaps.put(userName, "javax.persistence.jdbc.user");

    return keyMaps;
  }

  private void buildPersistenceOverridesMap(ConfigBuilder configBuilder) {
    // Hack for Hydra configs
    Map<String, String> keyMaps = mapHydraDbPropertiesToPersistence();

    final String cosmosRead = "cosmos_read";
    ConcurrentHashMap<String, String> cosmosReadOverrides = new ConcurrentHashMap<>();

    final String cosmosWrite = "cosmos_write";
    ConcurrentHashMap<String, String> cosmosWriteOverrides = new ConcurrentHashMap<>();

    final String audit = "audit";
    ConcurrentHashMap<String, String> auditOverrides = new ConcurrentHashMap<>();

    Action3<String, Entry<String, String>, ConcurrentHashMap> prepareOverrides = (dbName,
        mapEntry, overridesMap) -> {
      String key = mapEntry.getKey();
      String value = mapEntry.getValue();

      try {
        String propertyValue = configBuilder.getKeyAsObject(dbName + "." + key, String.class);
        if (propertyValue != null) {
          propertyValue = sanitizeString(propertyValue);
          if("javax.persistence.jdbc.url".equals(value)) {
            // randomise propertyValue
            propertyValue = SuperbiUtil.randomiseJDBCUrl(propertyValue);
          }
          overridesMap.put(value, propertyValue);
        }
      } catch (Exception e) {
        log.error("Error in providing overrides {} exception {}", dbName, e.getMessage());
        e.printStackTrace();
      }
    };

    keyMaps.entrySet().forEach(entry -> {
      prepareOverrides.call(cosmosRead, entry, cosmosReadOverrides);
      prepareOverrides.call(cosmosWrite, entry, cosmosWriteOverrides);
      prepareOverrides.call(audit, entry, auditOverrides);
    });

    this.persistenceOverridesMap.put(cosmosRead, cosmosReadOverrides);
    this.persistenceOverridesMap.put(cosmosWrite, cosmosWriteOverrides);
    this.persistenceOverridesMap.put(audit, auditOverrides);
    this.persistenceOverridesMap.put(cosmosRead, cosmosReadOverrides);
    this.persistenceOverridesMap.put(cosmosWrite, cosmosWriteOverrides);
    this.persistenceOverridesMap.put(audit, auditOverrides);
  }

  public boolean build(ConfigBuilder configBuilder) {
    log.info("Dynamic Config changed");
    try {
      // Hack for hydra
      sanitizeAllStrings();

      buildValidStoreList();
      buildSuperBiStoreMap(configBuilder);
      buildIronBankConfiguration();
      buildDruidClientConfiguration();
      buildTopicClusterMap();
      buildFStreamClientConfiguration();
      buildGringottsConfiguration();
//      buildResultStoreBucketConfig();
      buildD42Configuration();
      buildGcsConfiguration();
      buildConsumerRatioConfigs();
      buildDefaultBackgroundRefresherConfig();
      buildBackgroundRefresherConfigs();
      buildDedupeStoreTTLConfig();
      buildDedupeStoreConfig();
      buildPersistenceOverridesMap(configBuilder);
      buildKafkaConsumerConfig();
      buildKafkaProducerConfig();
      buildMaxCapacityConfigs();
      buildRetryConfigs();
      buildTTLAfterSuccessConfig();
      buildD42MetaConfig();
      buildTopicConfigRefMap();
      buildpubsubLiteInfraConfigMap();
      buildSubscriptionConfigRefMap();
      buildPrimarySecondaryConfig();



      log.info("Config object is build successfully");
      return true;
    } catch (Exception ex) {
      log.error("Error in building config object", ex);
    }

    return false;
  }

  @SneakyThrows
  private void sanitizeAllStrings() {
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      if (!field.getType().equals(String.class)) {
        // Ignore - Only string sanitization supported
        continue;
      }
      Sanitize annotation = field.getAnnotation(Sanitize.class);
      if (annotation != null) {
        boolean isAccessible = field.isAccessible();
        if (!isAccessible) {
          field.setAccessible(true);
        }
        Object valueObject = field.get(this);
        String value = (String) valueObject;
//            valueObject != null ? String.valueOf(valueObject) : null;
        field.set(this, sanitizeString(value));
        field.setAccessible(isAccessible);
      }
    }
  }

  @ConfigProperty("brv2.dBAuditorMaxThreads")
  private volatile String dBAuditorMaxThreads;

  @Override
  public int getDBAuditorMaxThreads() {
    return getIntValue(dBAuditorMaxThreads, 100);
  }

  @ConfigProperty("brv2.dBAuditerRequestTimeout")
  private volatile String dBAuditorRequestTimeout;

  @Override
  public int getDBAuditorRequestTimeout() {
    return getIntValue(dBAuditorRequestTimeout, 10000);
  }

  @Override
  public boolean isDBAuditorEnabled() {
    try {
      return Boolean.valueOf(enableDBAuditer);
    } catch (Exception ex) {
      log.error("Configuration error", ex);
    }
    return false;
  }

  @Override
  public boolean isAuditorEnabled() {
    try {
      return Boolean.valueOf(enableAudit);
    } catch (Exception ex) {
      log.error("Configuration error", ex);
    }
    return false;
  }

  @Override
  public Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    if (StringUtils.isNotBlank(persistenceUnit) && this.persistenceOverridesMap.containsKey(
        persistenceUnit)) {
      return this.persistenceOverridesMap.get(persistenceUnit);
    }
    return Maps.newHashMap();
  }

  @Override
  public Map<String,String> getTopicToInfraConfigRefMap() {
    if ( topicToInfraConfigRefMap == null || topicToInfraConfigRefMap.isEmpty() ) {
      log.error("No/Empty topic config ref found");
      throw new IllegalArgumentException("No/Empty topic config ref found");
    }
    return topicToInfraConfigRefMap;
  }

  @Override
  public Map<String, Map<String, Object>> getPubsubLiteInfraConfigMap() {
    if (pubsubLiteInfraConfigMap == null || pubsubLiteInfraConfigMap.isEmpty()) {
      log.error("No/Empty subscription infra config found for BRv2");
      throw new IllegalArgumentException("No/Empty subscription infra config found for BRv2");
    }
    return pubsubLiteInfraConfigMap;
  }

  @Override
  public MessageQueue getMessageQueue() {
    if ( StringUtils.isBlank(messageQueue) ) {
      log.error("No/Empty message Queue found");
      throw new IllegalArgumentException("No/Empty message Queue found");
    }
    try {
      return MessageQueue.valueOf(messageQueue);
    } catch (Exception ex) {
      log.error("Invalid message Queue Name : " + messageQueue);
      throw new IllegalArgumentException("Invalid message Queue Name : " + messageQueue);
    }
  }

  @Override
  public Map<String,List<String>> getSubscriptionConfigRefMap(){
    if (subscriptionConfigRefMap == null || subscriptionConfigRefMap.isEmpty()) {
      log.error("No/Empty subscription ConfigRef found for BRv2");
      throw new IllegalArgumentException("No/Empty subscription ConfigRef found for BRv2");
    }
    return subscriptionConfigRefMap;
  }

  @Override
  public Map<String,List<String>> getPrimarySecondarySubscriptionMap() {
    if (primarySecondarySubscriptionConfig == null || primarySecondarySubscriptionConfig.isEmpty()) {
      log.error("No/Empty primary secondary subscription mapping found for BRv2");
      throw new IllegalArgumentException("No/Empty primary secondary subscription mapping found for BRv2");
    }
    return primarySecondarySubscriptionConfig;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  @interface Sanitize {

  }
}
