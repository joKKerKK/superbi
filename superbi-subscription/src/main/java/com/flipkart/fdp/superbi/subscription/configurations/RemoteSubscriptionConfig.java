package com.flipkart.fdp.superbi.subscription.configurations;

import com.flipkart.fdp.config.ConfigBuilder;
import com.flipkart.fdp.config.ConfigProperty;
import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.gcs.GcsConfig;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.kloud.config.error.ConfigServiceException;
import com.google.common.collect.Maps;
import emailsvc.Connekt.ConnektServiceConfig;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Observable;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import rx.functions.Action3;

@Slf4j
public class RemoteSubscriptionConfig extends Observable implements SubscriptionConfig {

  @SneakyThrows
  public RemoteSubscriptionConfig() throws IOException, ConfigServiceException {
  }
  private static final String MAX_POOL_SIZE = "hibernate.c3p0.maxPoolSize";
  private static final String MAX_STATEMENT_SIZE = "hibernate.c3p0.maxStatements";

  @ConfigProperty("superbi.subscription.dataClient")
  private volatile String superbiClientStr;
  private SuperbiClientConfig superbiClientConfig;

  @ConfigProperty("superbi.subscription.platoMetaClient")
  private volatile String platoMetaClientStr;
  private PlatoMetaClientConfig platoMetaClientConfig;

  @ConfigProperty("superbi.subscription.platoExecutionClient")
  private volatile String platoExecutionClientStr;
  private PlatoExecutionClientConfig platoExecutionClientConfig;

  @ConfigProperty("superbi.d42.expiryInSeconds")
  private long d42ExpiryInSeconds;

  @ConfigProperty("superbi.c3p0.maxStatementsMultiplier")
  private volatile int maxStatementsMultiplier = 5;

  @ConfigProperty("subscription.DBAuditorRequestTimeout")
  private volatile int dbAuditorRequestTimeout = 10000;
  @ConfigProperty(("superbi.d42"))
  private volatile String d42ConfigStr;
  private D42Configuration d42Configuration;

  @ConfigProperty(("superbi.gcs"))
  private volatile String gcsConfigStr;
  private GcsConfig gcsConfig;

  @ConfigProperty(("superbi.gsheet"))
  private volatile String gsheetConfigStr;
  private GsheetConfig gsheetConfig;

  @ConfigProperty(("scheduler.config"))
  private volatile String schedulerConfigStr;
  private SchedulerConfig schedulerConfig;

  @ConfigProperty("superbi.connektServiceConfig")
  private volatile String connectConfigStr;
  private ConnektServiceConfig connektServiceConfig;

  @ConfigProperty("superbi.persistenceOverrides")
  private volatile String persistenceOverridesStr;
  private Map<String, Map<String, String>> persistenceOverridesMap = Maps
      .newHashMap();

  @ConfigProperty("subscription.audit.enable")
  private volatile String enableAudit = "true";

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
  public int getDBAuditorRequestTimeout() {
    return dbAuditorRequestTimeout;
  }

  @ConfigProperty("superbi.default.emailClient")
  private volatile String defaultEmailClientStr;
  private DefaultEmailConfig defaultEmailClientConfig;

  @ConfigProperty("subscription.email.template")
  private String emailTemplate;

  @ConfigProperty("subscription.email.gsheetCreationTemplate")
  private String gsheetCreationTemplate;

  @ConfigProperty("subscription.email.gsheetCancelledTemplate")
  private String gsheetCancelledTemplate;

  @ConfigProperty("subscription.email.gsheetOverwriteTemplate")
  private String gsheetOverwriteTemplate;

  @ConfigProperty("subscription.email.failureTemplate")
  private String failureEmailTemplate;

  @ConfigProperty("subscription.email.expirationCommTemplate")
  private String expirationCommTemplate;

  @ConfigProperty("subscription.jobConfig")
  private volatile String subscriptionJobConfigStr;
  private SubscriptionJobConfig subscriptionConfig;

  @ConfigProperty("subscription.maxRunsLeftForComm")
  private volatile int maxSubscriptionRunsLeftForComm;

  @ConfigProperty("subscription.maxDaysLeftForComm")
  private volatile int maxDaysLeftForComm;

  @SneakyThrows
  private void buildSusbcriptionJobConfig(){
    this.subscriptionConfig = JsonUtil.fromJson(subscriptionJobConfigStr,SubscriptionJobConfig.class);
    if(subscriptionConfig == null){
      throw new RuntimeException("Subscription Config can't be null");
    }
  }

  @SneakyThrows
  private void buildConnektServiceConfig(){
    this.connektServiceConfig = JsonUtil.fromJson(connectConfigStr,ConnektServiceConfig.class);
  }


  @SneakyThrows
  private void buildSchedulerConfig(){
    this.schedulerConfig = JsonUtil.fromJson(schedulerConfigStr,SchedulerConfig.class);
    if(schedulerConfig == null){
      throw new RuntimeException("Scheduler Config can't be null");
    }
  }

  @SneakyThrows
  public void buildD42Configuration() {
    this.d42Configuration = JsonUtil.fromJson(d42ConfigStr,D42Configuration.class);
    if(d42Configuration == null){
      throw new RuntimeException("D42 Config can't be null");
    }
  }

  @SneakyThrows
  public void buildGcsConfiguration() {
    this.gcsConfig = JsonUtil.fromJson(gcsConfigStr,GcsConfig.class);
    if(gcsConfig == null){
      throw new RuntimeException("Gcs Config can't be null");
    }
  }

  @SneakyThrows
  public void buildGsheetConfiguration() {
    this.gsheetConfig = JsonUtil.fromJson(gsheetConfigStr,GsheetConfig.class);
    if(gsheetConfig == null){
      throw new RuntimeException("Gsheet Config can't be null");
    }
  }

  @SneakyThrows
  private void buildSuperbiClientClientConfig(){
    this.superbiClientConfig = JsonUtil.fromJson(superbiClientStr, SuperbiClientConfig.class);
    if(superbiClientConfig == null){
      throw new RuntimeException("Superbi Client Config can't be null");
    }
  }

  @SneakyThrows
  private void buildPlatoMetaClientConfig() {
    this.platoMetaClientConfig = JsonUtil.fromJson(platoMetaClientStr, PlatoMetaClientConfig.class);
    if(platoMetaClientConfig == null){
      throw new RuntimeException("Plato Meta Client Config can't be null");
    }
  }

  @SneakyThrows
  private void buildPlatoExecutionClientConfig() {
    this.platoExecutionClientConfig = JsonUtil.fromJson(platoExecutionClientStr, PlatoExecutionClientConfig.class);
    if(platoExecutionClientConfig == null){
      throw new RuntimeException("Plato Execution Client Config can't be null");
    }
  }

  @SneakyThrows
  private void buildDefaultEmailClientConfig(){
    this.defaultEmailClientConfig = JsonUtil.fromJson(defaultEmailClientStr,DefaultEmailConfig.class);
  }

  private static String sanitizeString(String value) {
    if (StringUtils.isNotBlank(value)) {
      if (value.startsWith("\"") && value.endsWith("\"")) {
        return StringUtils.substring(value, 1, value.length() - 1);
      }
    }
    return value;
  }

  private void buildPersistenceOverridesMap(ConfigBuilder configBuilder) {

    Map<String, String> keyMaps = mapDbPropertiesToPersistence();

    final String hydra = "hydra";
    ConcurrentHashMap<String, String> hydraOverrides = new ConcurrentHashMap<>();

    Action3<String, Entry<String, String>, ConcurrentHashMap> prepareOverrides = (dbName,
        mapEntry, overridesMap) -> {
      String key = mapEntry.getKey();
      String value = mapEntry.getValue();

      try {
        String propertyValue = configBuilder.getKeyAsObject(dbName + "." + key, String.class);
        if (propertyValue != null) {
          overridesMap.put(value, sanitizeString(String.valueOf(propertyValue)));
        }
      } catch (Exception e) {
        log.error("Error in providing overrides {} exception {}", dbName, e.getMessage());
        e.printStackTrace();
      }
    };

    keyMaps.entrySet().forEach(entry -> {
      prepareOverrides.call(hydra,entry,hydraOverrides);
    });

    hydraOverrides.put("javax.persistence.jdbc.url",
        sanitizeString(configBuilder.getKeyAsObject("db.hydra.url", String.class)));
    hydraOverrides.put("javax.persistence.jdbc.user",
        sanitizeString(configBuilder.getKeyAsObject("db.hydra.user", String.class)));
    hydraOverrides.put("javax.persistence.jdbc.password",
        sanitizeString(configBuilder.getKeyAsObject("db.hydra.password", String.class)));
    hydraOverrides.put("hibernate.c3p0.acquireIncrement",
        sanitizeString(
            configBuilder.getKeyAsObject("db.hydra.acquireIncrement", String.class)));
    hydraOverrides.put("hibernate.c3p0.acquireRetryAttempts", sanitizeString(
        configBuilder.getKeyAsObject("db.hydra.acquireRetryAttempts", String.class)));
    hydraOverrides.put("hibernate.c3p0.maxPoolSize",
        sanitizeString(configBuilder.getKeyAsObject("db.hydra.maxPoolSize", String.class)));
    hydraOverrides.put("hibernate.c3p0.acquireRetryDelay",
        sanitizeString(
            configBuilder.getKeyAsObject("db.hydra.acquireRetryDelay", String.class)));
    hydraOverrides.put("hibernate.c3p0.timeout",
        sanitizeString(
            configBuilder.getKeyAsObject("db.hydra.connectionTimeout", String.class)));
    hydraOverrides.put(MAX_STATEMENT_SIZE,
        maxStatementsMultiplier * getIntValue(hydraOverrides.get(MAX_POOL_SIZE), 0) + "");
    this.persistenceOverridesMap.put(hydra,hydraOverrides);
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

  private Map<String, String> mapDbPropertiesToPersistence() {
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

  public SuperbiClientConfig getSuperbiClientConfig() {
    return superbiClientConfig;
  }

  @Override
  public PlatoMetaClientConfig getPlatoMetaClientConfig() {
    return platoMetaClientConfig;
  }

  @Override
  public PlatoExecutionClientConfig getPlatoExecutionClientConfig() {
    return platoExecutionClientConfig;
  }

  @Override
  public long getD42ExpiryInSeconds() {
    return d42ExpiryInSeconds;
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
  public SchedulerConfig getSchedulerConfig(){
    return schedulerConfig;
  }

  @Override
  public ConnektServiceConfig getConnektServiceConfig() {
    return connektServiceConfig;
  }

  @Override
  public int getMaxSubscriptionRunsLeftForComm() { return maxSubscriptionRunsLeftForComm; }

  @Override
  public int getMaxDaysLeftForComm() { return maxDaysLeftForComm; }

  @Override
  public SubscriptionJobConfig getSubscriptionJobConfig(){return subscriptionConfig;}
  public Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    if (StringUtils.isNotBlank(persistenceUnit) && this.persistenceOverridesMap.containsKey(
        persistenceUnit)) {
      return this.persistenceOverridesMap.get(persistenceUnit);
    }
    return Maps.newHashMap();
  }

  @Override
  public String getEmailTemplate(){
    return emailTemplate;
  }

  @Override
  public String getGsheetCreationEmailTemplate(){
    return gsheetCreationTemplate;
  }

  @Override
  public String getGsheetCancelledEmailTemplate(){
    return gsheetCancelledTemplate;
  }

  @Override
  public String getGsheetOverwriteEmailTemplate(){
    return gsheetOverwriteTemplate;
  }

  @Override
  public String getFailureEmailTemplate() {
    return failureEmailTemplate;
  }

  @Override
  public String getExpirationCommTemplate() {
    return expirationCommTemplate;
  }

  @Override
  public DefaultEmailConfig getDefaultEmailClientConfig(){
    return defaultEmailClientConfig;
  }

  @Override
  public GsheetConfig getGsheetConfig(){
    return gsheetConfig;
  }

  public boolean build(ConfigBuilder configBuilder) {
    log.info("Dynamic Config changed");
    try {
      sanitizeAllStrings();
      buildSuperbiClientClientConfig();
      buildPlatoMetaClientConfig();
      buildPlatoExecutionClientConfig();
      buildDefaultEmailClientConfig();
      buildD42Configuration();
      buildGcsConfiguration();
      buildGsheetConfiguration();
      buildSchedulerConfig();
      buildConnektServiceConfig();
      buildSusbcriptionJobConfig();
      buildPersistenceOverridesMap(configBuilder);
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

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  @interface Sanitize {

  }
}
