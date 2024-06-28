package com.flipkart.fdp.superbi.web.configurations;

import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.es.client.ESQuery.QueryType;
import com.flipkart.fdp.superbi.core.config.ApiKey;
import com.flipkart.fdp.superbi.core.config.CacheExpiryConfig;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClientConfiguration;
import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.exceptions.SuperBiException;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsConfiguration;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankConfiguration;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClientConfiguration;
import com.flipkart.fdp.superbi.http.client.qaas.QaasConfiguration;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableClient;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableConfig;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.config.D42MetaConfig;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.validation.BatchCubeGuardrailConfig;
import com.flipkart.fdp.superbi.utils.SuperbiUtil;
import com.flipkart.fdp.superbi.web.exception.ExceptionInfo;
import com.flipkart.fdp.utils.cfg.ConfigService;
import com.flipkart.fdp.utils.cfg.v2.DefaultConfigService;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import rx.functions.Action3;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.flipkart.fdp.superbi.cosmos.utils.Constants.BATCH_HDFS_OVERRIDE_KEY;
import static com.flipkart.fdp.superbi.cosmos.utils.Constants.BATCH_OVERRIDE_KEY;
import static com.flipkart.fdp.superbi.cosmos.utils.Constants.REALTIME_OVERRIDE_KEY;
import static com.flipkart.fdp.superbi.cosmos.utils.Constants.TARGET_FACT_OVERRIDE_KEY;

/**
 * Created by akshaya.sharma on 23/07/19
 */
@Slf4j
@NoArgsConstructor
public class RemoteApplicationConfig extends Observable implements ApplicationConfig {

  private Gson gson = new Gson();
  private ConfigService configService;

  @SneakyThrows
  public RemoteApplicationConfig(ConfigService configService) {
    this.configService = configService;
    this.gson = new Gson();
  }

  public void setConfigService(DefaultConfigService defaultConfigService) {
    this.configService = defaultConfigService;
  }

  private static final String MAX_POOL_SIZE = "hibernate.c3p0.maxPoolSize";
  private static final String MAX_STATEMENT_SIZE = "hibernate.c3p0.maxStatements";

  private volatile int maxStatementsMultiplier = 5;

  private volatile int executorServiceThreadCount;

  private String dataSourcesStr;
  private List<String> dataSourcesList;

  private volatile String apiKeysString = "[]";
  private List<ApiKey> apiKeys;

  private volatile String factRefreshTimeRequiredStr = "[]";
  private List<String> factRefreshTimeRequiredList;

  private volatile String clientPrivilegeString;
  private Map<String,ClientPrivilege> clientPrivilegeMap= Maps.newConcurrentMap();

  private volatile String defaultClientPrivilegeStr;
  private ClientPrivilege defaultClientPrivilege;

//  @Sanitize
  private String d42MetaConfigStr;
  private D42MetaConfig d42MetaConfig;

  private volatile String batchCubeGuardrailConfigStr;
  private BatchCubeGuardrailConfig batchCubeGuardrailConfig;


  private String elasticSearchCostBoostStr;
  private Map<ESQuery.QueryType,Long> elasticSearchCostBoost;

  @SneakyThrows
  private void buildElasticSearchCostBoost(){
    this.elasticSearchCostBoostStr =
        getConfigKey("superbi.elasticSearch.costBoost", String.class);
    this.elasticSearchCostBoost = gson.fromJson(elasticSearchCostBoostStr
        ,new TypeToken<ConcurrentHashMap<ESQuery.QueryType,Long>>() {
    }.getType());
  }

  private void buildD42MetaConfig(){
    this.d42MetaConfigStr = getSanitizedConfigKey("superbi.d42MetaConfig");
    this.d42MetaConfig = gson.fromJson(d42MetaConfigStr,D42MetaConfig.class);
  }

  private void buildBatchCubeGuardrailConfig(){
    this.batchCubeGuardrailConfigStr = getSanitizedConfigKey("superbi.batchCubeGuardrailConfig");
    this.batchCubeGuardrailConfig = gson.fromJson(batchCubeGuardrailConfigStr, BatchCubeGuardrailConfig.class);
  }

  private void buildFactRefreshTimeRequired(){
    try{
      this.factRefreshTimeRequiredStr =
          getConfigKey("superbi.factRefreshTimeRequired", String.class);
      if (factRefreshTimeRequiredStr == null)
        factRefreshTimeRequiredStr = "[]";
      this.factRefreshTimeRequiredList = gson.fromJson(factRefreshTimeRequiredStr,List.class);
    }catch (Exception e){
      this.factRefreshTimeRequiredList = new ArrayList<>();
    }

  }

  @Override
  public D42MetaConfig getD42MetaConfig() {
    if(d42MetaConfig == null){
      log.error("Invalid D42MetaConfig");
      throw new RuntimeException("D42MetaConfig can't be null");
    }
    return d42MetaConfig;
  }

  @Override
  public BatchCubeGuardrailConfig getBatchCubeGuardrailConfig() {
    if(batchCubeGuardrailConfig == null) {
      log.error("Invalid BatchCubeGuardrailConfig");
      throw new RuntimeException("BatchCubeGuardrailConfig can't be null");
    }
    return batchCubeGuardrailConfig;
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
      log.error("Invalid fStream configuration");
      throw new RuntimeException("Invalid fStream configuration");
    }
    return fStreamClientConfiguration;
  }

  @Override
  public BadgerClientConfiguration getBadgerClientConfiguration() {
    if (badgerClientConfiguration == null) {
      log.error("Invalid badger configuration");
      throw new RuntimeException("Invalid badger configuration");
    }
    return badgerClientConfiguration;
  }

  @Override
  public QaasConfiguration getQaasConfiguration() {
    if (qaasConfiguration == null) {
      log.error("Invalid Qaas configuration");
      throw new RuntimeException("Invalid Qaas configuration");
    }
    return qaasConfiguration;
  }

  @SneakyThrows
  private void buildClientPrivileges(){
    this.clientPrivilegeString = getConfigKey("superbi.client.privilege", String.class);
    this.clientPrivilegeMap = gson.fromJson(clientPrivilegeString,new TypeToken<ConcurrentHashMap<String, ClientPrivilege>>() {
    }.getType());
  }

  @SneakyThrows
  private void buildDefaultClientPrivilege(){
    this.defaultClientPrivilegeStr = getConfigKey("superbi.client.defaultPrivilege", String.class);
    this.defaultClientPrivilege = gson.fromJson(defaultClientPrivilegeStr,ClientPrivilege.class);
  }

  @SneakyThrows
  private void buildApiKeys() {
    this.apiKeysString = getConfigKey("superbi.apiKeys", String.class);
    if(apiKeysString == null)
      apiKeysString = "[]";
    this.apiKeys = Arrays.asList(gson.fromJson(apiKeysString, ApiKey[].class));
  }

  private volatile String sourceRunningInSafeModeConfigsStr;
  private Map<String, Boolean> sourceRunningInSafeModeMap = Maps.newConcurrentMap();

  private Map<String, DataSource> superbiStoreMap = Maps.newConcurrentMap();

  public void buildSuperBiStoreMap() {
    this.dataSourcesList.forEach(i -> this.superbiStoreMap.put(String.valueOf(i),
      gson.fromJson(getConfigKey("superbi.dataSource.".concat(i), String.class),
            DataSource.class)));
  }

  private void buildValidStoreList() {
    this.dataSourcesStr = getConfigKey("superbi.dataSources", String.class);
    this.dataSourcesList = gson.fromJson(this.dataSourcesStr, List.class);
  }

  @SneakyThrows
  private void buildSourceRunningInSafeModeDynamicConfig() {
    this.sourceRunningInSafeModeConfigsStr = getConfigKey("superbi.source.safeMode", String.class);
    this.sourceRunningInSafeModeMap = gson.fromJson(sourceRunningInSafeModeConfigsStr,
        new TypeToken<ConcurrentHashMap<String, Boolean>>() {
        }.getType());
  }

//
//  @ConfigProperty("cachestore.nodes")
//  @Sanitize
//  private volatile String couchbaseNodes = "";

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

  private <T> T getConfigKey(String keyName, Class<T> keyClass){
    return configService.getKey(keyName, keyClass);
  }

  private String getSanitizedConfigKey(String keyName){
    return sanitizeString(getConfigKey(keyName, String.class));
  }

//  couchbase config not needed in gcp
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

  private volatile String bigTableConfigStr;
  private BigTableConfig bigTableConfig;

//  @Sanitize
  private volatile String resultTable = "query_results";

  @Sanitize
  private volatile String lockTable = "lock_store";

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
  public void buildBigtableConfig() {
    this.bigTableConfigStr = getConfigKey("bigtable.config",String.class);
    this.bigTableConfig = gson.fromJson(this.bigTableConfigStr, BigTableConfig.class);
  }

  private BigtableDataClient getBigTableDataClient() throws IOException {
    return this.getBigTableConfig().getBigTableDataClient();
  }

  private BigTableConfig getBigTableConfig() {
    return bigTableConfig;
  }

  public String getResultTable(){
    this.resultTable = getSanitizedConfigKey("query_results.tableId");
    if(null == resultTable)
      resultTable = "query_results";
    return resultTable;
  }

  @Override
  public BigTableClient getResultTableClient() throws IOException {
    return new BigTableClient(getBigTableDataClient(), getResultTable());
  }

  @Override
  public BigTableClient getLockTableClient() throws IOException {
    return new BigTableClient(getBigTableDataClient(), getLockTable());
  }

  private String getLockTable() {
    this.lockTable = getSanitizedConfigKey("lock_store.tableId");
    if(null ==  lockTable)
      lockTable = "lock_store";
    return lockTable;
  }

//  @ConfigProperty("lock_store.name")
//  @Sanitize
//  private volatile String lockBucket = "lock_store";
//  @ConfigProperty("lock_store.password")
//  @Sanitize
//  private volatile String lockBucketPassword;
//  @ConfigProperty("lock_store.ObsTimeoutMS")
//  @Sanitize
//  private volatile String lockBucketObsTimeoutMS;
//  @ConfigProperty("lock_store.OpTimeoutMS")
//  @Sanitize
//  private volatile String lockBucketOpTimeoutMS;
//  @ConfigProperty("lock_store.OpQueueMaxBlockTimeMS")
//  @Sanitize
//  private volatile String lockBucketOpQueueMaxBlockTimeMS;
//  private CouchbaseBucketConfig lockStoreBucketConfig;

//  @SneakyThrows
//  private void buildLockStoreBucketConfig() {
//    this.lockStoreBucketConfig = new CouchbaseBucketConfig(
//        couchbaseNodes.split(","), lockBucket, lockBucketPassword,
//        getIntValue(lockBucketObsTimeoutMS),
//        getIntValue(lockBucketOpTimeoutMS),
//        getIntValue(lockBucketOpQueueMaxBlockTimeMS)
//    );
//  }


  /*
  CacheExpiry
   */
  private volatile String cacheExpiryConfigsStr;
  private Map<String, CacheExpiryConfig> cacheExpiryConfigMap = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildCacheExpiryDynamicConfigs() {
    this.cacheExpiryConfigsStr = getConfigKey("superbi.cacheExpirayConfigs", String.class);
    this.cacheExpiryConfigMap = gson.fromJson(cacheExpiryConfigsStr,
        new TypeToken<ConcurrentHashMap<String, CacheExpiryConfig>>() {
        }.getType());
  }

  private volatile String defaultCacheExpirayConfigStr;
  private CacheExpiryConfig defaultCacheExpirayConfig = new CacheExpiryConfig(0, 0, 0, 0);

  @SneakyThrows
  private void buildDefaultCacheExpiryDynamicConfig() {
    this.defaultCacheExpirayConfigStr = getConfigKey("superbi.defaultCacheExpirayConfig",
        String.class);
    this.defaultCacheExpirayConfig = gson.fromJson(defaultCacheExpirayConfigStr,
        CacheExpiryConfig.class);
  }


  /**
   * Kafka Producer Config for BRv1.
   */
  private volatile String kafkaProducerConfigStr;
  private Map<String, Object> kafkaProducerConfig = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildKafkaProducerConfig() {
    this.kafkaProducerConfigStr = getConfigKey("superbi.kafkaProducerConfig", String.class);
    this.kafkaProducerConfig = gson.fromJson(kafkaProducerConfigStr,
        new TypeToken<ConcurrentHashMap<String, Object>>() {
        }.getType());
  }

  /*
  BackgroundRefresherConfig
   */
  private volatile String backgroundRefresherConfigsStr;
  private Map<String, BackgroundRefresherConfig> backgroundRefresherConfigMap = Maps
      .newConcurrentMap();

  @SneakyThrows
  private void buildBackgroundRefresherConfigs() {
    this.backgroundRefresherConfigsStr = getConfigKey("superbi.backgroundRefresherConfigs",
        String.class);
    this.backgroundRefresherConfigMap = gson.fromJson(backgroundRefresherConfigsStr,
        new TypeToken<ConcurrentHashMap<String, BackgroundRefresherConfig>>() {
        }.getType());
  }

  private volatile String defaultBackgroundRefresherConfigStr;
  private BackgroundRefresherConfig defaultBackgroundRefresherConfig = new
      BackgroundRefresherConfig(
      500, 100000, 100, 2000, 200, 3600 * 24,
      2, 5000, 10 * 1024 * 1024L, 0.0d,1024);

  @SneakyThrows
  private void buildDefaultBackgroundRefresherConfig() {
    this.defaultBackgroundRefresherConfigStr = getConfigKey(
        "superbi.defaultBackgroundRefresherConfig", String.class);
    this.defaultBackgroundRefresherConfig = gson.fromJson(defaultBackgroundRefresherConfigStr,
        BackgroundRefresherConfig.class);
  }

  @Override
  public CacheExpiryConfig getCacheExpiryConfig(String storeIdentifier) {
    if (StringUtils.isNotBlank(storeIdentifier) && cacheExpiryConfigMap.containsKey(
        storeIdentifier)) {
      return cacheExpiryConfigMap.get(storeIdentifier);
    }
    return defaultCacheExpirayConfig;
  }

  @Override
  public List<String> getDataSourcesList() {
    return this.dataSourcesList;
  }

  @Override
  public Boolean isSourceRunningInSafeMode(String storeIdentifier) {
    try {
      if (StringUtils.isNotBlank(storeIdentifier) && sourceRunningInSafeModeMap.containsKey(
          storeIdentifier)) {
        return sourceRunningInSafeModeMap.get(storeIdentifier);
      }
    } catch (Exception ex) {
      log.error("Configuration error", ex);
    }
    return false;
  }

  @Override
  public List<ApiKey> getApiKeys() {
    return apiKeys;
  }

  public ClientPrivilege getClientPrivilege(String clientId){
    return clientPrivilegeMap.get(clientId) != null ?  clientPrivilegeMap.get(clientId) : defaultClientPrivilege;
  }

//  @Override
//  public CouchbaseBucketConfig getResultStoreBucketConfig() {
//    return this.resultStoreBucketConfig;
//  }

//  @Override
//  public CouchbaseBucketConfig getLockStoreBucketConfig() {
//    return this.lockStoreBucketConfig;
//  }

  @Override
  public BackgroundRefresherConfig getBackgroundRefresherConfig(String storeIdentifier) {
    if (StringUtils.isNotBlank(storeIdentifier) && backgroundRefresherConfigMap.containsKey(
        storeIdentifier)) {
      return backgroundRefresherConfigMap.get(storeIdentifier);
    }
    return defaultBackgroundRefresherConfig;
  }

  private volatile String gringottsStr;
  private GringottsConfiguration gringottsConfiguration;

  @SneakyThrows
  private void buildGringottsConfiguration() {
    this.gringottsStr = getConfigKey("superbi.gringotts", String.class);
    this.gringottsConfiguration = gson.fromJson(gringottsStr, GringottsConfiguration.class);
  }

  private volatile String ironBankStr;
  private IronBankConfiguration ironBankConfiguration;

  private volatile String druidClientStr;
  private DruidClientConfiguration druidClientConfiguration;

  private volatile String mmgClientStr;
  private MmgClientConfiguration mmgClientConfiguration;

  private volatile String fStreamClientStr;
  private FStreamClientConfiguration fStreamClientConfiguration;

  private volatile String badgerClientStr;
  private BadgerClientConfiguration badgerClientConfiguration;

  private volatile String qaasClientStr;
  private QaasConfiguration qaasConfiguration;

  @SneakyThrows
  private void buildIronBankConfiguration() {
    this.ironBankStr = getConfigKey("superbi.ironbank", String.class);
    this.ironBankConfiguration = gson.fromJson(ironBankStr, IronBankConfiguration.class);
  }

  @SneakyThrows
  private void buildDruidClientConfiguration() {
    this.druidClientStr = getConfigKey("superbi.druidClient", String.class);
    this.druidClientConfiguration = gson.fromJson(druidClientStr, DruidClientConfiguration.class);
  }

  @SneakyThrows
  private void buildMmgClientConfiguration(){
    this.mmgClientStr = getConfigKey("superbi.mmgClient", String.class);
    this.mmgClientConfiguration = gson.fromJson(mmgClientStr, MmgClientConfiguration.class);
  }

  @SneakyThrows
  private void buildFStreamClientConfiguration() {
    this.fStreamClientStr = getConfigKey("superbi.fStreamClient", String.class);
    this.fStreamClientConfiguration = gson.fromJson(fStreamClientStr, FStreamClientConfiguration.class);
  }

  @SneakyThrows
  private void buildQaasClientConfiguration(){
    this.qaasClientStr = getConfigKey("superbi.qaasClient", String.class);
    this.qaasConfiguration = gson.fromJson(qaasClientStr, QaasConfiguration.class);
  }

  @Override
  public GringottsConfiguration getGringottsConfiguration() {
    if (gringottsConfiguration == null) {
      log.error("Invalid Gringotts config");
      throw new RuntimeException("Invalid Gringotts config");
    }
    return gringottsConfiguration;
  }

  @Override
  public IronBankConfiguration getIronBankConfiguration() {
    if (ironBankConfiguration == null) {
      log.error("Invalid IronBankConfiguration config");
      throw new RuntimeException("Invalid IronBankConfiguration config");
    }
    return ironBankConfiguration;
  }

  @Override
  public MmgClientConfiguration getMmgClientConfiguration() {
    if (mmgClientConfiguration == null) {
      log.error("Invalid MmgConfiguration");
      throw new RuntimeException("Invalid MmgConfiguration");
    }
    return mmgClientConfiguration;
  }

  @Override
  public Long getElasticSearchCostBoost(QueryType queryType) {
    return elasticSearchCostBoost.get(queryType) == null ? 1L : elasticSearchCostBoost.get(queryType);
  }

  private volatile String checkDSQuerySerializationStr;
  private Map<String, Boolean> checkDSQuerySerialization = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildDSQuerySerializationConfig() {
    this.checkDSQuerySerializationStr = getConfigKey("superbi.checkDSQuerySerialization",
        String.class);
    this.checkDSQuerySerialization = gson
        .fromJson(checkDSQuerySerializationStr, new TypeToken<ConcurrentHashMap<String, Boolean>>() {
        }.getType());
  }


  private static final boolean DEFAULT_CHECK_DSQUERY_SERIALIZATION = false;

  @Override
  public boolean checkDSQuerySerialization(String storeIdentifier) {
    if(checkDSQuerySerialization == null)
      return DEFAULT_CHECK_DSQUERY_SERIALIZATION;
    return checkDSQuerySerialization.getOrDefault(storeIdentifier, DEFAULT_CHECK_DSQUERY_SERIALIZATION);
  }

  private volatile String calculatePriorityStr;
  private Map<String, Boolean> calculatePriorityMap = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildCalculatePriorityMap() {
    this.calculatePriorityStr = getConfigKey("superbi.calculatePriority", String.class);
    this.calculatePriorityMap = gson
        .fromJson(calculatePriorityStr, new TypeToken<ConcurrentHashMap<String, Boolean>>() {
        }.getType());
  }

  private static final boolean DEFAULT_SHOULD_CALCULATE_PRIORITY = false;

  @Override
  public boolean shouldCalculatePriority(String storeIdentifier) {
    if(calculatePriorityStr == null)
      return DEFAULT_SHOULD_CALCULATE_PRIORITY;
    return calculatePriorityMap.getOrDefault(storeIdentifier, DEFAULT_SHOULD_CALCULATE_PRIORITY);
  }

  @Override
  public Map<String, Object> getKafkaProducerConfig() {
    if (kafkaProducerConfig == null || kafkaProducerConfig.isEmpty()) {
      log.error("Invalid Kafka Producer config for SuperBi");
      throw new RuntimeException("Invalid Kafka Producer config for SuperBi");
    }
    return kafkaProducerConfig;
  }

  private volatile String d42ConfigStr;
  private D42Configuration d42Configuration;

  public void buildD42Configuration() {
    this.d42ConfigStr = getConfigKey("superbi.d42", String.class);
    this.d42Configuration = gson.fromJson(this.d42ConfigStr, D42Configuration.class);
  }

  @Override
  public D42Configuration getD42Configuration() {
    return d42Configuration;
  }

  @Override
  public Map<String, DataSource> getDataSourceMap() {
    return superbiStoreMap;
  }

  @Override
  public boolean getFactRefreshTimeRequired(String dataSource){
    return factRefreshTimeRequiredList.contains(dataSource);
  }
  @Override
  public int getExecutorServiceThreadCount() {
    this.executorServiceThreadCount = getIntValue(getConfigKey("superbi.executorService.threadCount", String.class));
    return executorServiceThreadCount;
  }

  private volatile int concurrencyForScriptEngine = 5;

  private volatile String exceptionInfoMapStr = "{}";

  @Getter
  private Map<String, ExceptionInfo> exceptionInfoMap = Maps.newConcurrentMap();

  private void buildExceptionMappings() {
    this.exceptionInfoMapStr = getConfigKey("superbi.exceptionInfoMapStr", String.class);
    if(exceptionInfoMapStr == null)
      exceptionInfoMapStr = "{}";
    this.exceptionInfoMap = gson.fromJson(exceptionInfoMapStr, new
        TypeToken<ConcurrentHashMap<String, ExceptionInfo>>() {
        }.getType());
  }

  @Override
  public int getConcurrencyForScriptEngine() {
    this.concurrencyForScriptEngine = getIntValue(getConfigKey("superbi.concurrencyForScriptEngine", String.class),5);
    return concurrencyForScriptEngine;
  }

  /*
  Persistence Overrides
   */
  private volatile String persistenceOverridesStr;
  private Map<String, Map<String, String>> persistenceOverridesMap = Maps
      .newHashMap();


  /*
  add superbi.MessageQueue, superbi.topicToInfraConfigRefMap,
  brv2.pubsubLiteInfraConfig in both superbi & Brv2 config bucket
   */
  private volatile String messageQueue;

  private volatile String topicToInfraConfigRefStr;
  private Map<String,String> topicToInfraConfigRefMap;

  @SneakyThrows
  private void buildTopicConfigRefMap() {
    this.topicToInfraConfigRefStr = getConfigKey("superbi.topicToInfraConfigRefMap", String.class);
    this.topicToInfraConfigRefMap = gson.fromJson(topicToInfraConfigRefStr,
        new TypeToken<ConcurrentHashMap<String, String>>() {
        }.getType());
  }

  private volatile String pubsubLiteInfraConfigStr;
  private Map<String, Map<String, Object>> pubsubLiteInfraConfigMap = Maps.newConcurrentMap();

  @SneakyThrows
  private void buildPubsubLiteInfraConfig() {
    this.pubsubLiteInfraConfigStr = getConfigKey("brv2.pubsubLiteInfraConfig", String.class);
    this.pubsubLiteInfraConfigMap = gson.fromJson(pubsubLiteInfraConfigStr,
        new TypeToken<ConcurrentHashMap<String, Map<String,Object>>>() {
        }.getType());
  }

  private Map<String, String> mapHydraDbPropertiesToPersistence() {
    Map<String, String> keyMaps = new HashMap<>();
    String jdbcUrl = "database.jdbcUrl";
    keyMaps.put(jdbcUrl, "javax.persistence.jdbc.url");

    String acquireIncrement = "database.acquireIncrement";
    keyMaps.put(acquireIncrement, "hibernate.c3p0.acquireIncrement");

    String maxPoolSize = "database.maxPoolSize";
    keyMaps.put(maxPoolSize, "hibernate.c3p0.maxPoolSize");

    // TODO
//    String partitionCount = "database.partitionCount";
//    keyMaps.put(partitionCount, "");

//    String minConnectionsPerPartition = "database.minConnectionsPerPartition";
//    keyMaps.put(minConnectionsPerPartition, "");

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

  private void buildPersistenceOverridesMap() {
    // Hack for Hydra configs
    Map<String, String> keyMaps = mapHydraDbPropertiesToPersistence();

    final String cosmosRead = "cosmos_read";
    ConcurrentHashMap<String, String> cosmosReadOverrides = new ConcurrentHashMap<>();

    final String cosmosWrite = "cosmos_write";
    ConcurrentHashMap<String, String> cosmosWriteOverrides = new ConcurrentHashMap<>();

    final String audit = "audit";
    ConcurrentHashMap<String, String> auditOverrides = new ConcurrentHashMap<>();

    final String hydra = "hydra_read";
    ConcurrentHashMap<String, String> hydraOverrides = new ConcurrentHashMap<>();

    Action3<String, Map.Entry<String, String>, ConcurrentHashMap> prepareOverrides = (dbName,
        mapEntry, overridesMap) -> {
      String key = mapEntry.getKey();
      String value = mapEntry.getValue();

      try {
        String propertyValue;
        try {
          propertyValue = getConfigKey(dbName + "." + key, String.class);
        } catch (ClassCastException c) {
          propertyValue = String.valueOf(getConfigKey(dbName + "." + key, Integer.class));
        }
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
      prepareOverrides.call(hydra, entry, hydraOverrides);
    });

    maxStatementsMultiplier = getIntValue(getConfigKey("superbi.c3p0.maxStatementsMultiplier", String.class),5);
    /*
  Persistence Overrides
   */
    persistenceOverridesStr = getConfigKey("superbi.persistenceOverrides", String.class);

    // Hydra hack
    String hydraJdbcUrl = getSanitizedConfigKey("db.hydra_read.url");
    hydraJdbcUrl = SuperbiUtil.randomiseJDBCUrl(hydraJdbcUrl);
    hydraOverrides.put("javax.persistence.jdbc.url",hydraJdbcUrl);
    hydraOverrides.put("javax.persistence.jdbc.user",
        getSanitizedConfigKey("db.hydra_read.user"));
    hydraOverrides.put("javax.persistence.jdbc.password",
        getSanitizedConfigKey("db.hydra_read.password"));
    hydraOverrides.put("hibernate.c3p0.acquireIncrement",
        getSanitizedConfigKey("db.hydra_read.acquireIncrement"));
    hydraOverrides.put("hibernate.c3p0.acquireRetryAttempts",
        getSanitizedConfigKey("db.hydra_read.acquireRetryAttempts"));
    hydraOverrides.put("hibernate.c3p0.maxPoolSize",
        getSanitizedConfigKey("db.hydra_read.maxPoolSize"));
    hydraOverrides.put("hibernate.c3p0.acquireRetryDelay",
        getSanitizedConfigKey("db.hydra_read.acquireRetryDelay"));
    hydraOverrides.put("hibernate.c3p0.timeout",
        getSanitizedConfigKey("db.hydra_read.connectionTimeout"));

    cosmosReadOverrides.put(MAX_STATEMENT_SIZE,
        maxStatementsMultiplier * getIntValue(cosmosReadOverrides.get(MAX_POOL_SIZE), 0) + "");
    cosmosWriteOverrides.put(MAX_STATEMENT_SIZE,
        maxStatementsMultiplier * getIntValue(cosmosWriteOverrides.get(MAX_POOL_SIZE), 0) + "");
    hydraOverrides.put(MAX_STATEMENT_SIZE,
        maxStatementsMultiplier * getIntValue(hydraOverrides.get(MAX_POOL_SIZE), 0) + "");

    this.persistenceOverridesMap.put(cosmosRead, cosmosReadOverrides);
    this.persistenceOverridesMap.put(cosmosWrite, cosmosWriteOverrides);
    this.persistenceOverridesMap.put(hydra, hydraOverrides);
    this.persistenceOverridesMap.put(audit, auditOverrides);
  }

  public boolean build() {
    log.info("Dynamic Config changed");
    try {
      // Hack for hydra
      sanitizeAllStrings();

      buildApiKeys();
      buildClientPrivileges();
      buildValidStoreList();
      buildDefaultClientPrivilege();
      buildGringottsConfiguration();
      buildIronBankConfiguration();
      buildDruidClientConfiguration();
      buildMmgClientConfiguration();
      buildFStreamClientConfiguration();
      buildBadgerClientConfiguration();
      buildQaasClientConfiguration();
      buildSuperBiStoreMap();
      buildPersistenceOverridesMap();

//      buildResultStoreBucketConfig();
//      buildLockStoreBucketConfig();
      buildDefaultCacheExpiryDynamicConfig();
      buildD42Configuration();
      buildCacheExpiryDynamicConfigs();
      buildSourceRunningInSafeModeDynamicConfig();
      buildDSQuerySerializationConfig();
      buildDefaultBackgroundRefresherConfig();
      buildBackgroundRefresherConfigs();
      buildExceptionMappings();
      buildKafkaProducerConfig();
      buildD42MetaConfig();
      buildBatchCubeGuardrailConfig();
      buildCalculatePriorityMap();
      buildElasticSearchCostBoost();
      buildFactRefreshTimeRequired();
      buildDataSourceAttrMap();
      buildTableNameEnrichStoreIdentifierMap();
      buildTopicConfigRefMap();
      buildPubsubLiteInfraConfig();
      buildBigtableConfig();

      log.info("Config object is build successfully");
      return true;
    } catch (Exception ex) {
      log.error("Error in building config object", ex);
    }

    return false;
  }

  @SneakyThrows
  private void buildBadgerClientConfiguration() {
    this.badgerClientStr = getConfigKey("superbi.badgerClient", String.class);
    this.badgerClientConfiguration = gson.fromJson(badgerClientStr, BadgerClientConfiguration.class);
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

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  @interface Sanitize {

  }

  @Override
  public Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    if (StringUtils.isNotBlank(persistenceUnit) && this.persistenceOverridesMap.containsKey(
        persistenceUnit)) {
      return this.persistenceOverridesMap.get(persistenceUnit);
    }
    return Maps.newHashMap();
  }

  private volatile String dBAuditerMaxThreads;

  @Override
  public int getDBAuditerMaxThreads() {
    this.dBAuditerMaxThreads = getConfigKey("superbi.dBAuditerMaxThreads", String.class);
    return getIntValue(dBAuditerMaxThreads,100);
  }

  private volatile String dBAuditerRequestTimeout;

  @Override
  public int getDBAuditerRequestTimeout() {
    this.dBAuditerRequestTimeout = getConfigKey("superbi.dBAuditerRequestTimeout", String.class);
    return getIntValue(dBAuditerRequestTimeout, 10000);
  }

  private volatile String enableDBAuditer = "false";

  @Override
  public boolean isDBAuditerEnabled() {
    try {
      this.enableDBAuditer = getConfigKey("AuditStoreDbLogger.enable", String.class);
      if(enableDBAuditer == null)
        enableDBAuditer = "false";
      return Boolean.valueOf(enableDBAuditer);
    } catch (Exception ex) {
      log.error("Configuration error", ex);
    }
    return false;
  }


  private volatile String enableAudit = "false";

  @Override
  public boolean isSuperBiAuditerEnabled() {
    try {
      this.enableAudit = getConfigKey("audit.enable", String.class);
      if(enableAudit == null) {
        enableAudit = "false";
      }
      return Boolean.valueOf(enableAudit);
    } catch (Exception ex) {
      log.error("Configuration error", ex);
    }
    return false;
  }

  private Map<String, Map<String, String>> dataSourceAttributesMap = Maps.newConcurrentMap();

  public void buildDataSourceAttrMap() {
    dataSourceAttributesMap = superbiStoreMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
              Map<String, String> attribute = e.getValue().getAttributes();
              return attribute == null ? new HashMap<>() : attribute;
            }));
  }

  @Override
  public Map<String, Map<String, String>> getDataSourceAttributes() {
    return dataSourceAttributesMap;
  }

  private volatile String tableNameEnrichStoreIdentifiersStr;

  private Map<String, List<String>> tableNameEnrichStoreIdentifierMap = new HashMap<>();

  private void buildTableNameEnrichStoreIdentifierMap() {
    this.tableNameEnrichStoreIdentifiersStr = getConfigKey("superbi.table_name.enrich.store_identifiers", String.class);
    this.tableNameEnrichStoreIdentifierMap = gson.fromJson(tableNameEnrichStoreIdentifiersStr, Map.class);
  }

  @Override
  public Map<String, List<String>> getStoreIdentifiersForTableEnrich() {
    if (!tableNameEnrichStoreIdentifierMap.containsKey(BATCH_OVERRIDE_KEY) || tableNameEnrichStoreIdentifierMap.get(BATCH_OVERRIDE_KEY).size() == 0) {
      throw new SuperBiException("superbi.table_name.enrich.store_identifiers.BATCH_OVERRIDE isn't defined in config bucket or doesn't have any value");
    }

    if (!tableNameEnrichStoreIdentifierMap.containsKey(REALTIME_OVERRIDE_KEY) || tableNameEnrichStoreIdentifierMap.get(REALTIME_OVERRIDE_KEY).size() == 0) {
      throw new SuperBiException("superbi.table_name.enrich.store_identifiers.REALTIME_OVERRIDE isn't defined in config bucket or doesn't have any value");
    }

    if (!tableNameEnrichStoreIdentifierMap.containsKey(BATCH_HDFS_OVERRIDE_KEY) || tableNameEnrichStoreIdentifierMap.get(BATCH_HDFS_OVERRIDE_KEY).size() == 0) {
      throw new SuperBiException("superbi.table_name.enrich.store_identifiers.BATCH_HDFS_OVERRIDE_KEY isn't defined in config bucket or doesn't have any value");
    }

    if (!tableNameEnrichStoreIdentifierMap.containsKey(TARGET_FACT_OVERRIDE_KEY) || tableNameEnrichStoreIdentifierMap.get(TARGET_FACT_OVERRIDE_KEY).size() == 0) {
      throw new SuperBiException("superbi.table_name.enrich.store_identifiers.TARGET_FACT_OVERRIDE_KEY isn't defined in config bucket or doesn't have any value");
    }
    return tableNameEnrichStoreIdentifierMap;
  }

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
    this.messageQueue = getConfigKey("superbi.messageQueue", String.class);
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
}
