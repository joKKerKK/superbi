package com.flipkart.fdp.superbi.brv2;

import static org.asynchttpclient.Dsl.asyncHttpClient;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.audit.dao.AuditDao;
import com.flipkart.fdp.compito.api.cache.DedupeStore;
import com.flipkart.fdp.compito.api.cache.impl.InMemoryDedupeStore;
import com.flipkart.fdp.compito.api.clients.RecordConverter;
import com.flipkart.fdp.compito.api.clients.consumer.DefaultPollStrategy;
import com.flipkart.fdp.compito.api.clients.consumer.PollStrategy;
import com.flipkart.fdp.compito.api.clients.producer.Producer;
import com.flipkart.fdp.compito.api.clients.producer.ProducerCallback;
import com.flipkart.fdp.compito.api.clients.producer.ProducerRecord;
import com.flipkart.fdp.compito.api.command.CommandFactory;
import com.flipkart.fdp.compito.api.request.Message;
import com.flipkart.fdp.compito.core.CommandScheduler;
import com.flipkart.fdp.compito.core.CompitoEngine;
import com.flipkart.fdp.compito.kafka.DefaultConsumerFactory;
import com.flipkart.fdp.compito.kafka.DefaultKafkaProducer;
import com.flipkart.fdp.compito.kafka.DefaultKafkaRecordConverter;
import com.flipkart.fdp.compito.pubsublite.DefaultPubsubLiteProducer;
import com.flipkart.fdp.compito.pubsublite.DefaultPubsubLiteRecordConverter;
import com.flipkart.fdp.compito.pubsublite.PubsubLiteConsumerFactory;
import com.flipkart.fdp.compito.pubsublite.PubsubLiteFailSafePublisher;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.util.EntityManagerProvider;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.brv2.config.ApplicationConfig;
import com.flipkart.fdp.superbi.brv2.config.BRv2Config;
import com.flipkart.fdp.superbi.brv2.config.BRv2ServiceConfiguration;
import com.flipkart.fdp.superbi.brv2.config.CircuitBreakerProperties;
import com.flipkart.fdp.superbi.brv2.config.DataSource;
import com.flipkart.fdp.superbi.brv2.config.EnvironmentConfig;
import com.flipkart.fdp.superbi.brv2.config.HealthCheckConfig;
import com.flipkart.fdp.superbi.brv2.config.ext.DynamicRemoteConfig;
import com.flipkart.fdp.superbi.brv2.config.ext.LocalConfig;
import com.flipkart.fdp.superbi.brv2.execution.DummyLockDao;
import com.flipkart.fdp.superbi.brv2.execution.DummyRetryTaskHandler;
import com.flipkart.fdp.superbi.brv2.execution.SuperBiCommandFactory;
import com.flipkart.fdp.superbi.brv2.factory.BigQueryDataSourceFactory;
import com.flipkart.fdp.superbi.brv2.factory.DataSourceFactory;
import com.flipkart.fdp.superbi.brv2.factory.DruidDataSourceFactory;
import com.flipkart.fdp.superbi.brv2.factory.ElasticSearchDataSourceFactory;
import com.flipkart.fdp.superbi.brv2.factory.FStreamDataSourceFactory;
import com.flipkart.fdp.superbi.brv2.factory.HdfsDataSourceFactory;
import com.flipkart.fdp.superbi.brv2.factory.VerticaDataSourceFactory;
import com.flipkart.fdp.superbi.brv2.logger.AuditDBLogger;
import com.flipkart.fdp.superbi.brv2.logger.AuditFileLogger;
import com.flipkart.fdp.superbi.brv2.logger.Auditor;
import com.flipkart.fdp.superbi.brv2.logger.CompositeAuditor;
import com.flipkart.fdp.superbi.brv2.resource.HealthCheck;
import com.flipkart.fdp.superbi.brv2.services.HealthCheckService;
import com.flipkart.fdp.superbi.brv2.util.ClassScannerUtil;
import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaModifier;
import com.flipkart.fdp.superbi.cosmos.queryTranslator.CosmosNativeQueryTranslator;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.d42.D42Uploader;
import com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor;
import com.flipkart.fdp.superbi.execution.BackgroundRefreshTaskExecutor;
import com.flipkart.fdp.superbi.execution.BasicQueryExecutor;
import com.flipkart.fdp.superbi.execution.FailureStreamConsumer;
import com.flipkart.fdp.superbi.execution.RetryTaskHandler;
import com.flipkart.fdp.superbi.execution.SuccessStreamConsumer;
import com.flipkart.fdp.superbi.gcs.GcsConfig;
import com.flipkart.fdp.superbi.gcs.GcsUploader;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsConfiguration;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankClient;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankConfiguration;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.models.TopicInfraConfig;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.BigTableCacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.InMemoryCacheDao;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClient;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClient;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.resilienthttpclient.ResilientDomain;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudRegionOrZone;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.metrics.CircuitBreakerMetrics;
import io.github.resilience4j.metrics.RetryMetrics;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.validation.constraints.NotNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.hibernate.SessionFactory;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.threeten.bp.Duration;

@Slf4j
public class BackgroundRefresherV2Module extends AbstractModule {

  private final BRv2ServiceConfiguration configService;
  private final EnvironmentConfig environmentConfig;
  private final ApplicationConfig config;


  @SneakyThrows
  BackgroundRefresherV2Module(BRv2ServiceConfiguration configService) {
    this.configService = configService;
    this.environmentConfig = configService.getEnvironmentConfig();

    ApplicationConfig applicationConfig = DynamicRemoteConfig.getInstance();

    if ("local".equals(this.environmentConfig.getEnv())) {
      this.config = new LocalConfig(applicationConfig);
    } else {
      this.config = applicationConfig;
    }
  }

  private void bindInScope(Set<Class> classes, Class<? extends Annotation> scopeAnnotation) {
    classes.forEach(clazz -> {
      bindInScope(clazz, scopeAnnotation);
    });
  }

  private void bindInScope(Class clazz, Class<? extends Annotation> scopeAnnotation) {
    if (isInstantiable(clazz)) {
      bind(clazz).in(scopeAnnotation);
    }
  }

  @SneakyThrows
  private Set<Class> getClasses(final String packageName, Class... parentClasses) {
    return ClassScannerUtil.getClasses(packageName, parentClasses);
  }

  private boolean isInstantiable(Class clazz) {
    boolean isAbstract = Modifier.isAbstract(clazz.getModifiers());
    boolean isInterface = clazz.isInterface();
    return !(isAbstract || isInterface);
  }

  @SneakyThrows
  private void bindToImpl(Class inf) {
    Class implClass = Class.forName(
        inf.getPackage().getName() + ".impl." + inf.getSimpleName() + "Impl");
    if (isInstantiable(implClass) && inf.isAssignableFrom(implClass)) {
      bind(inf).to(implClass);
    }
  }


  @Provides
  @Named("AUDIT")
  private GetEntityManagerFunction<GenericDAO, EntityManager> getAuditEntityManagerFunction() {
    final String persistenceUnit = "audit";
    Map<String, String> overrides = config.getPersistenceOverrides(persistenceUnit);

    return dao -> {
      return EntityManagerProvider.getEntityManager(persistenceUnit, overrides);
    };
  }


  @Provides
  @Named("COSMOS")
  private GetEntityManagerFunction<GenericDAO, EntityManager> getCosmosEntityManagerFunction() {
    final String persistenceUnit = "cosmos_read";
    Map<String, String> overrides = config.getPersistenceOverrides(persistenceUnit);
    return dao -> {
      return EntityManagerProvider.getEntityManager(persistenceUnit, overrides);
    };
  }

  protected void initializeCosmos() {
    final String persistenceUnitCosmosRead = "cosmos_read";
    Map<String, String> persistenceOverridesCosmosRead = config
        .getPersistenceOverrides(persistenceUnitCosmosRead);

    EntityManagerFactory entityManagerFactory = new HibernatePersistenceProvider()
        .createEntityManagerFactory(persistenceUnitCosmosRead, persistenceOverridesCosmosRead);
    SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);
    MetaAccessor.initialize(sessionFactory);

    final String persistenceUnitCosmosWrite = "cosmos_write";
    Map<String, String> persistenceOverridesCosmosWrite = config
        .getPersistenceOverrides(persistenceUnitCosmosWrite);

    EntityManagerFactory entityManagerFactoryWrite = new HibernatePersistenceProvider()
        .createEntityManagerFactory(persistenceUnitCosmosWrite, persistenceOverridesCosmosWrite);
    SessionFactory sessionFactoryWrite = entityManagerFactoryWrite.unwrap(SessionFactory.class);

    MetaCreator.initialize(sessionFactoryWrite);
    MetaModifier.initialize(sessionFactoryWrite);
  }


  @Provides
  @Named("SERVICE")
  private MetricRegistry getMetricRegistry(Injector injector) {
    return injector.getInstance(MetricRegistry.class);
  }

  @Provides
  @Named("RESULT_STORE")
  private CacheDao getAttemptStore(CacheDao cacheDao) {
    return cacheDao;
  }

  @Provides
  @Named("HANDLE_STORE")
  private CacheDao getHandleStore(CacheDao cacheDao) {
    return cacheDao;
  }


  @Provides
  @Named("JOB_STORE")
  private CacheDao getJobStore(CacheDao cacheDao) {
    return new CacheDao() {

      private String getJobCacheKey(String key) {
        return StringUtils.join("bqjob", key, '_');
      }

      @Override
      public <T extends JsonSerializable> Optional<T> get(String k, Class<T> valueClass) {
        return cacheDao.get(getJobCacheKey(k), valueClass);
      }

      @Override
      public void set(String k, int ttl, JsonSerializable v) {
        cacheDao.set(getJobCacheKey(k), ttl, v);
      }

      @Override
      public void add(String k, int ttl, JsonSerializable v) {
        cacheDao.add(getJobCacheKey(k), ttl, v);
      }

      @Override
      public void remove(String k) {
        cacheDao.remove(getJobCacheKey(k));
      }
    };
  }


  @Override
  @SneakyThrows
  protected void configure() {
    try {
      bind(BRv2ServiceConfiguration.class).toInstance(configService);
      bind(HealthCheckConfig.class).toInstance(configService.getHealthCheckConfig());
      bind(EnvironmentConfig.class).toInstance(environmentConfig);
      bind(HealthCheckService.class).asEagerSingleton();

      // ResourceClases
      Set<Class> resourceClasses = getClasses(HealthCheck.class.getPackage().getName(),
          Object.class);
      bindInScope(resourceClasses, Singleton.class);

      /**
       * Bind Audit Dao from audit-db dependency
       */
      Set<Class> auditDaos = getClasses(AuditDao.class.getPackage().getName(),
          GenericDAO.class);
      auditDaos.forEach((clazz) -> {
        if (clazz.isInterface()) {
          bindToImpl(clazz);
        }
      });

      bind(BRv2Config.class).toInstance(config);

      bind(AuditDBLogger.class).in(Singleton.class);
      bind(AuditFileLogger.class).in(Singleton.class);

      initializeCosmos();

      JSScriptEngineAccessor.initScriptEngine(config.getConcurrencyForScriptEngine());
      com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor.initScriptEngine(
          config.getConcurrencyForScriptEngine());

      //Audit DB
      Map<String, String> overrides_audit = config.getPersistenceOverrides("audit");
      EntityManagerProvider.getEntityManager("audit", overrides_audit);
      log.info("AUDIT DB EMF built");

    } catch (Exception ex) {
      log.error("Error while initialising guice :", ex);
      throw ex;
    }

  }

  @NotNull
  @Provides
  @Singleton
  @Named("AUDITORS")
  private List<Auditor> getAuditersList(AuditDBLogger auditDBLogger,
      AuditFileLogger auditFileLogger) {
    return Lists.newArrayList(auditDBLogger, auditFileLogger);
  }

  @NotNull
  @Provides
  @Singleton
  private Auditor getAuditer(CompositeAuditor compositeAuditor) {
    return compositeAuditor;
  }

  @NotNull
  @Provides
  @Singleton
  private ExecutionAuditor getExecutionAuditor(CompositeAuditor compositeAuditor) {
    return compositeAuditor;
  }

  @Provides
  @Singleton
  private org.apache.kafka.clients.producer.Producer<String, SuperBiMessage> providesKafkaProducer() {
    return new KafkaProducer<>(config.getKafkaProducerConfig());
  }

  private Entry<String, PubsubLiteFailSafePublisher> getTopicNameToFailSafePublisherEntry(
      String topicName,
      TopicInfraConfig topicInfraConfig) {

    String cloudRegion = topicInfraConfig.getCloudRegion();
    Character cloudZoneId = topicInfraConfig.getCloudZoneId();
    Long projectId = topicInfraConfig.getProjectId();

    CloudRegionOrZone location;
    if ( cloudZoneId == null ) {
      location = CloudRegionOrZone.of(CloudRegion.of(cloudRegion));
    } else {
      location = CloudRegionOrZone.of(
          CloudZone.of(CloudRegion.of(cloudRegion), cloudZoneId));
    }


    TopicPath topicPath =
        TopicPath.newBuilder()
            .setProject(ProjectNumber.of(projectId))
            .setLocation(location)
            .setName(TopicName.of(topicName))
            .build();

    BatchingSettings batchingSettings = BatchingSettings.newBuilder()
          .setIsEnabled(topicInfraConfig.isBatchEnabled())
          .setElementCountThreshold(topicInfraConfig.getElementCountThreshold())
          .setRequestByteThreshold(topicInfraConfig.getRequestByteThreshold())
          .setDelayThreshold(Duration.ofMillis(topicInfraConfig.getDelayThreshold()))
          .build();

    PublisherSettings publisherSettings =
        PublisherSettings.newBuilder()
            .setTopicPath(topicPath)
            .setBatchingSettings(batchingSettings)
            .build();

    return Maps.immutableEntry(topicName, new PubsubLiteFailSafePublisher(publisherSettings));
  }

  @NotNull
  @Provides
  @Singleton
  private Map<String, PubsubLiteFailSafePublisher> providesPubsubTopicMap( ) {
    Map<String,PubsubLiteFailSafePublisher> pubsubTopicFailSafePublisherMap = new HashMap<>();
    Map<String, String> topicToInfraConfigRefMap = config.getTopicToInfraConfigRefMap();
    Map<String, Map<String, Object>> pubsubLiteInfraConfigMap = config
        .getPubsubLiteInfraConfigMap();
    Map<String, TopicInfraConfig> enrichedTopicInfraConfigMap = pubsubLiteInfraConfigMap.entrySet().stream()
        .collect(
            Collectors.toMap(Entry::getKey, topicConfig -> {
              try {
                String configJson = JsonUtil.toJson(topicConfig.getValue());
                return JsonUtil.fromJson(configJson, TopicInfraConfig.class);
              } catch (Exception e) {
                throw new IllegalArgumentException(
                    String.format("Unable to parse topic infra configs. Cause %s",
                        e.getMessage()), e);
              }
            }));
    topicToInfraConfigRefMap.entrySet().forEach(entry -> {
      String topicName = entry.getKey();
      String infraConfig = entry.getValue();
      try {
        Entry<String, PubsubLiteFailSafePublisher> topicNameToPublisherEntry = getTopicNameToFailSafePublisherEntry(topicName,
            enrichedTopicInfraConfigMap.get(infraConfig));
        pubsubTopicFailSafePublisherMap.put(topicNameToPublisherEntry.getKey(), topicNameToPublisherEntry.getValue());
      } catch (Exception e) {
        log.error("Error creating pubsub topic to publisher map : {}", e);
        throw new RuntimeException(e);
      }
    });
    return pubsubTopicFailSafePublisherMap;
  }

  @NotNull
  @Provides
  @Singleton
  private Producer<String, SuperBiMessage> providesProducer(
      Map<String, PubsubLiteFailSafePublisher> topicToFailSafePublisherMap,
      org.apache.kafka.clients.producer.Producer<String, SuperBiMessage> kafkaProducer) {
    if ( config.getMessageQueue() == MessageQueue.PUBSUB_LITE) {
      return new DefaultPubsubLiteProducer<>(topicToFailSafePublisherMap,
          this::pubsubKeyDeserializer,
          this::pubsubValueDeserializer);
    } else {
      return new DefaultKafkaProducer<>(kafkaProducer);
    }
  }

  private String pubsubKeyDeserializer(ProducerRecord<String,SuperBiMessage> producerRecord) {
    return producerRecord.key();
  }

  private ByteString pubsubValueDeserializer(Message<SuperBiMessage> message) {
    return ByteString.copyFromUtf8(JsonUtil.toJson(message));
  }

  @NotNull
  @Provides
  @Singleton
  private ProducerCallback<String, SuperBiMessage> providesProducerCallback() {
    return new ProducerErrorCallback();
  }

  @NotNull
  @Provides
  @Singleton
  private SuccessStreamConsumer providesSuccessStreamConsumer(D42Uploader d42Uploader,
      MetricRegistry metricRegistry, CacheDao cacheDao, GcsUploader gcsUploader) {
    return new SuccessStreamConsumer(cacheDao, d42Uploader, gcsUploader, metricRegistry,
        config.getD42MetaConfig());
  }

  @Provides
  @Singleton
  public D42Uploader provideD42Uploader(MetricRegistry metricRegistry) {

    D42Configuration d42Configuration = config.getD42Configuration();

    D42Client d42Client = new D42Client(d42Configuration.getD42Endpoint(),
        d42Configuration.getAccessKey(),
        d42Configuration.getSecretKey(), d42Configuration.getBucket(), d42Configuration.getProjectId());
    return new D42Uploader(d42Client, getCircuitBreakerForD42Uploader(metricRegistry),
        getRetryForD42Uploader(metricRegistry), metricRegistry);
  }

  @Provides
  @Singleton
  public GcsUploader provideGcsUploader(MetricRegistry metricRegistry) {

    GcsConfig gcsConfig = config.getGcsConfig();
    return new GcsUploader(gcsConfig, metricRegistry);
  }

  private Retry getRetryForD42Uploader(MetricRegistry metricRegistry) {
    RetryConfig retryConfig = config.getD42Configuration()
        .getCircuitBreakerProperties().getRetryConfig();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    Retry retry = registry.retry(D42Uploader.class.getName());
    RetryMetrics.ofRetryRegistry(registry, metricRegistry);
    return retry;
  }

  private CircuitBreaker getCircuitBreakerForD42Uploader(MetricRegistry metricRegistry) {
    CircuitBreakerConfig circuitBreakerConfig = config.getD42Configuration()
        .getCircuitBreakerProperties().getCircuitBreakerConfig();
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    CircuitBreaker circuitBreaker = registry.circuitBreaker(D42Uploader.class.getName());
    CircuitBreakerMetrics.ofCircuitBreakerRegistry(registry, metricRegistry);
    return circuitBreaker;
  }

  @NotNull
  @Provides
  @Singleton
  private FailureStreamConsumer providesFailureStreamConsumer(MetricRegistry metricRegistry,
      CacheDao cacheDao) {
    return new FailureStreamConsumer(cacheDao, metricRegistry);
  }


  @NotNull
  @Provides
  @Singleton
  private RetryTaskHandler providesRetryTaskHandler() {
    return new DummyRetryTaskHandler();
  }


  @NotNull
  @Provides
  @Singleton
  private LockDao providesLockDAO() {
    return new DummyLockDao();
  }


  @NotNull
  @Provides
  @Singleton
  @Named("QueryExecutorService")
  private ExecutorService getQueryExecutorService() {
    return Executors.newFixedThreadPool(config.getExecutorServiceThreadCount());
  }

  @Provides
  @Singleton
  public GringottsClient prepareGringottsClient(MetricRegistry metricRegistry) {
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForGringotts(
        GringottsClient.SERVICE_NAME, config.getGringottsConfiguration(),
        metricRegistry);
    return new GringottsClient(asyncHttpClient, config.getGringottsConfiguration());
  }

  private AsyncHttpClient createAsyncHttpClientForGringotts(String name,
      GringottsConfiguration gringottsConfiguration,
      MetricRegistry metricRegistry) {
    return ResilientDomain.builder(name)
        .circuitBreakerConfig(
            gringottsConfiguration.getCircuitBreakerProperties().getCircuitBreakerConfig())
        .bulkheadConfig(gringottsConfiguration.getCircuitBreakerProperties().getBulkheadConfig())
        .asyncHttpClient(asyncHttpClient(
            Dsl.config().setMaxConnections(gringottsConfiguration.getMaxConnectionsTotal())
                .setMaxConnectionsPerHost(gringottsConfiguration.getMaxConnectionPerHost())
                .setPooledConnectionIdleTimeout(
                    gringottsConfiguration.getPooledConnectionIdleTimeout())
                .setConnectionTtl(gringottsConfiguration.getConnectionTtlInMillis())
                .setConnectTimeout(gringottsConfiguration.getConnectTimeout())
                .setRequestTimeout(gringottsConfiguration.getRequestTimeoutInMillies())
                .build()))
        .metricRegistry(metricRegistry).build();
  }

  @Provides
  @Singleton
  public IronBankClient prepareIronBankClient(MetricRegistry metricRegistry) {
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForIronBank(IronBankClient.SERVICE_NAME,
        config.getIronBankConfiguration(),
        metricRegistry);
    return new IronBankClient(asyncHttpClient, config.getIronBankConfiguration());
  }

  private AsyncHttpClient createAsyncHttpClientForIronBank(String name,
      IronBankConfiguration ironBankConfiguration,
      MetricRegistry metricRegistry) {
    return ResilientDomain.builder(name)
        .circuitBreakerConfig(
            ironBankConfiguration.getCircuitBreakerProperties().getCircuitBreakerConfig())
        .bulkheadConfig(ironBankConfiguration.getCircuitBreakerProperties().getBulkheadConfig())
        .asyncHttpClient(asyncHttpClient(
            Dsl.config().setMaxConnections(ironBankConfiguration.getMaxConnectionsTotal())
                .setMaxConnectionsPerHost(ironBankConfiguration.getMaxConnectionPerHost())
                .setPooledConnectionIdleTimeout(
                    ironBankConfiguration.getPooledConnectionIdleTimeout())
                .setConnectionTtl(ironBankConfiguration.getConnectionTtlInMillis())
                .setConnectTimeout(ironBankConfiguration.getConnectTimeout())
                .setRequestTimeout(ironBankConfiguration.getRequestTimeoutInMillies())
                .build()))
        .metricRegistry(metricRegistry).build();
  }

  @NotNull
  @Provides
  @Singleton
  private BackgroundRefreshTaskExecutor providesBackgroundRefreshTaskExecutor(
      BasicQueryExecutor basicQueryExecutor, SuccessStreamConsumer successStreamConsumer,
      FailureStreamConsumer failureStreamConsumer,
      @Named("QueryExecutorService") ExecutorService executorService,
      ExecutionAuditor executionAuditor,
      MetricRegistry metricRegistry) {
    return new BackgroundRefreshTaskExecutor(basicQueryExecutor, successStreamConsumer,
        failureStreamConsumer, executorService, executionAuditor, metricRegistry);
  }

  @Provides
  @Singleton
  @Named("DataSourceFactory")
  public Map<DataSourceType, DataSourceFactory> getDataSourceFactoryMap(
      HdfsDataSourceFactory hdfsDataSourceFactory,
      ElasticSearchDataSourceFactory elasticSearchDataSourceFactory,
      VerticaDataSourceFactory verticaDataSourceFactory,
      FStreamDataSourceFactory fStreamDataSourceFactory,
      DruidDataSourceFactory druidDataSourceFactory,
      BigQueryDataSourceFactory bigQueryDataSourceFactory
  ) {

    Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap = new HashMap<>();
    dataSourceFactoryMap.put(DataSourceType.ELASTIC_SEARCH, elasticSearchDataSourceFactory);
    dataSourceFactoryMap.put(DataSourceType.VERTICA, verticaDataSourceFactory);
    dataSourceFactoryMap.put(DataSourceType.FSTREAM, fStreamDataSourceFactory);
    dataSourceFactoryMap.put(DataSourceType.HDFS, hdfsDataSourceFactory);
    dataSourceFactoryMap.put(DataSourceType.DRUID, druidDataSourceFactory);
    dataSourceFactoryMap.put(DataSourceType.BIG_QUERY, bigQueryDataSourceFactory);
    return dataSourceFactoryMap;
  }

  @Provides
  @Singleton
  public DruidDataSourceFactory getDruidDataSourceFactory(DruidClient druidClient) {
    return new DruidDataSourceFactory(druidClient);
  }

  @Provides
  @Singleton
  public ElasticSearchDataSourceFactory getElasticSearchDataSourceFactory() {
    return new ElasticSearchDataSourceFactory();
  }

  @Provides
  @Singleton
  public VerticaDataSourceFactory getVerticaDataSourceFactory() {
    return new VerticaDataSourceFactory();
  }

  @Provides
  @Singleton
  public FStreamDataSourceFactory getFStreamDataSourceFactory(FStreamClient fStreamClient) {
    return new FStreamDataSourceFactory(fStreamClient);
  }

  @Provides
  @Singleton
  public HdfsDataSourceFactory getHdfsDataSourceFactory(@Named("HANDLE_STORE") CacheDao cacheDao,
      GringottsClient gringottsClient, IronBankClient ironBankClient) {
    return new HdfsDataSourceFactory(cacheDao, gringottsClient, ironBankClient);
  }

  @Provides
  @Singleton
  public BigQueryDataSourceFactory getBigQueryDataSourceFactory(
      @Named("JOB_STORE") CacheDao cacheDao
      , GringottsClient gringottsClient, MetricRegistry metricRegistry) {
    return new BigQueryDataSourceFactory(cacheDao, gringottsClient, metricRegistry);
  }

  @Provides
  @Singleton
  public DruidClient prepareDruidClient(MetricRegistry metricRegistry) {
    DruidClientConfiguration configuration = config.getDruidClientConfiguration();
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForDruid(DruidClient.SERVICE_NAME,
        config.getDruidClientConfiguration(),
        metricRegistry);
    return new DruidClient(asyncHttpClient, configuration.getBasePath(), configuration.getPort(),
        configuration.getClientSideExceptions());
  }

  private AsyncHttpClient createAsyncHttpClientForDruid(String serviceName,
      DruidClientConfiguration druidClientConfiguration, MetricRegistry metricRegistry) {
    return ResilientDomain.builder(serviceName)
        .circuitBreakerConfig(
            druidClientConfiguration.getCircuitBreakerProperties().getCircuitBreakerConfig())
        .bulkheadConfig(druidClientConfiguration.getCircuitBreakerProperties().getBulkheadConfig())
        .asyncHttpClient(asyncHttpClient(
            Dsl.config().setMaxConnections(druidClientConfiguration.getMaxConnectionsTotal())
                .setMaxConnectionsPerHost(druidClientConfiguration.getMaxConnectionPerHost())
                .setPooledConnectionIdleTimeout(
                    druidClientConfiguration.getPooledConnectionIdleTimeout())
                .setConnectionTtl(druidClientConfiguration.getConnectionTtlInMillis())
                .setConnectTimeout(druidClientConfiguration.getConnectTimeout())
                .setRequestTimeout(druidClientConfiguration.getRequestTimeoutInMillies())
                .setReadTimeout(druidClientConfiguration.getReadTimeoutInMillies())
                .build()))
        .metricRegistry(metricRegistry).build();
  }

  @Provides
  @Singleton
  public FStreamClient prepareFStreamClient(MetricRegistry metricRegistry) {
    FStreamClientConfiguration configuration = config.getFStreamClientConfiguration();
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForFStream(FStreamClient.SERVICE_NAME,
        config.getFStreamClientConfiguration(),
        metricRegistry);
    return new FStreamClient(asyncHttpClient, configuration.getBasePath(), configuration.getPort());
  }

  private AsyncHttpClient createAsyncHttpClientForFStream(String serviceName,
      FStreamClientConfiguration fStreamClientConfiguration, MetricRegistry metricRegistry) {
    return ResilientDomain.builder(serviceName)
        .circuitBreakerConfig(
            fStreamClientConfiguration.getCircuitBreakerProperties().getCircuitBreakerConfig())
        .bulkheadConfig(
            fStreamClientConfiguration.getCircuitBreakerProperties().getBulkheadConfig())
        .asyncHttpClient(asyncHttpClient(
            Dsl.config().setMaxConnections(fStreamClientConfiguration.getMaxConnectionsTotal())
                .setMaxConnectionsPerHost(fStreamClientConfiguration.getMaxConnectionPerHost())
                .setPooledConnectionIdleTimeout(
                    fStreamClientConfiguration.getPooledConnectionIdleTimeout())
                .setConnectionTtl(fStreamClientConfiguration.getConnectionTtlInMillis())
                .setConnectTimeout(fStreamClientConfiguration.getConnectTimeout())
                .setRequestTimeout(fStreamClientConfiguration.getRequestTimeoutInMillies())
                .build()))
        .metricRegistry(metricRegistry).build();
  }

  @NotNull
  @Provides
  @Singleton
  private BasicQueryExecutor providesBasicQueryExecutor(
      @Named("DaoProvider") Map<String, DataSourceDao> daoProvider,MetricRegistry metricRegistry) {
    return new BasicQueryExecutor(daoProvider,
        metricRegistry);
  }

  @Provides
  @Singleton
  @SneakyThrows
  private CacheDao cacheDaoProvider(MetricRegistry metricRegistry) {
    if ("local".equals(this.environmentConfig.getEnv())) {
      return new InMemoryCacheDao(1000, 30, TimeUnit.DAYS);
    }
    return new BigTableCacheDao(config.getResultTableClient(), metricRegistry);
//    } else {
//      Bucket resultStoreBucket = config.getResultStoreBucketConfig().getCouchbaseBucket();
//      return new CouchbaseCacheDao(resultStoreBucket, metricRegistry);
//    }
  }

  @Provides
  @Singleton
  private RecordConverter<String, SuperBiMessage> providesRecordConverter() {
    if ( config.getMessageQueue() == MessageQueue.PUBSUB_LITE ) {
      return new DefaultPubsubLiteRecordConverter<>(this::generateTopicNameFromSuperBiMessage);
    } else {
      return new DefaultKafkaRecordConverter<>(this::generateTopicNameFromSuperBiMessage);
    }
  }

  private String generateTopicNameFromSuperBiMessage(SuperBiMessage superBiMessage) {
    String storeIdentifier = superBiMessage.getQueryPayload().getStoreIdentifier();
    return storeIdentifier.concat("-").concat("retry");
  }


  @Provides
  @Singleton
  @SneakyThrows
  private DedupeStore<String, SuperBiMessage> providesDedupeStore() {
    return new InMemoryDedupeStore<>(config.getDedupeStoreConfig().getMaxSize(),
        config.getDedupeStoreConfig().getExpiryTimeInMin(), TimeUnit.MINUTES);
  }

  @Provides
  @Singleton
  @SneakyThrows
  @Named("CommandFactory")
  private CommandFactory<String, SuperBiMessage, QueryResult> providesCommandFactory(
      Producer<String, SuperBiMessage> producer,
      DedupeStore<String, SuperBiMessage> dedupeStore,
      ProducerCallback<String, SuperBiMessage> producerCallback,
      RecordConverter<String, SuperBiMessage> converter,
      BackgroundRefreshTaskExecutor backgroundRefreshTaskExecutor,
      LockDao lockDao, RetryTaskHandler retryTaskHandler, MetricRegistry metricRegistry) {
    return new SuperBiCommandFactory(dedupeStore, config::getTTLAfterSuccessInMillis, producer,
        producerCallback, converter, backgroundRefreshTaskExecutor,
        config::getBackgroundRefresherConfig, config::getRetryConfig, lockDao, retryTaskHandler,
        metricRegistry);
  }

  @Provides
  @Singleton
  @SneakyThrows
  private Map<String, CommandScheduler> providesCommandSchedulerMap(
      @Named("CommandFactory") CommandFactory<String, SuperBiMessage, QueryResult> commandFactory,
      DedupeStore<String, SuperBiMessage> dedupeStore, MetricRegistry metricRegistry,
      CircuitBreakerRegistry circuitBreakerRegistry) {

    Map<String, CommandScheduler> commandSchedulerMap = Maps
        .newHashMap();

    for (Entry<String, DataSource> dataSourceEntry : config.getDataSourceMap().entrySet()) {
      String dataSourceId = dataSourceEntry.getKey();
      AtomicLong maxCapacity = new AtomicLong(0);
      AtomicLong remainingCapacity = new AtomicLong(0);
      AtomicLong requestsInProcess = new AtomicLong(0);
      AtomicLong commandSchedulerState = new AtomicLong(1);
      metricRegistry
          .register(getMetricNameForMaxCapacity(dataSourceId), (Gauge<Long>) maxCapacity::get);
      metricRegistry.register(getMetricNameForRemainingCapacity(dataSourceId),
          (Gauge<Long>) remainingCapacity::get);
      metricRegistry.register(getMetricNameForRequestsInProcess(dataSourceId),
          (Gauge<Long>) requestsInProcess::get);
      CommandScheduler<String, SuperBiMessage, QueryResult> commandScheduler = new CommandScheduler<>(
          dataSourceId, commandFactory, dedupeStore, config.getDedupeStoreTTLInMs(dataSourceId),
          config::getMaxCapacity, metricRegistry, remainingCapacity, maxCapacity, requestsInProcess,
          commandSchedulerState,
          circuitBreakerRegistry.circuitBreaker(dataSourceId, dataSourceId));
      commandSchedulerMap.put(dataSourceId, commandScheduler);
    }

    Map<String, Consumer<Double>> updateMaxCapacityMap = commandSchedulerMap.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()::updateMaxCapacity));

    PubsubLiteConsumerFactory<String, SuperBiMessage> pubsubLiteConsumerFactory =
        getPubsubLiteConsumerFactory(updateMaxCapacityMap,metricRegistry);

    DefaultConsumerFactory<String, SuperBiMessage> consumerFactory =
        getKafkaConsumerFactory(updateMaxCapacityMap,metricRegistry);

    for (Entry<String, DataSource> dataSourceEntry : config.getDataSourceMap().entrySet()) {
      String dataSourceId = dataSourceEntry.getKey();
      CommandScheduler<String, SuperBiMessage, QueryResult> commandScheduler = commandSchedulerMap.get(dataSourceId);
      if ( config.getMessageQueue() == MessageQueue.PUBSUB_LITE ) {
        commandScheduler.setConsumer(
            pubsubLiteConsumerFactory.create(config.getConsumerRatioConfig(dataSourceId)));
      } else {
        commandScheduler.setConsumer(
            consumerFactory.create(config.getConsumerRatioConfig(dataSourceId)));
      }
    }

    return commandSchedulerMap;
  }

  private DefaultConsumerFactory<String, SuperBiMessage> getKafkaConsumerFactory(
      Map<String, Consumer<Double>> updateMaxCapacityMap,
      MetricRegistry metricRegistry) {
    Map<String, Integer> maxSizeOfInternalBufferMap = new HashMap<>();
    Map<String, Integer> minSizeOfInternalBufferMap = new HashMap<>();
    Map<String, List<String>> topicClusterMap = config.getTopicClusterMap();
    for (Entry<String, DataSource> dataSourceEntry : config.getDataSourceMap().entrySet()) {
      String dataSourceId = dataSourceEntry.getKey();
      // TODO : make it config driven
      minSizeOfInternalBufferMap.put(dataSourceId, 200);
      maxSizeOfInternalBufferMap.put(dataSourceId, 500);
    }
    return new DefaultConsumerFactory<String, SuperBiMessage>(
        new DefaultPollStrategy<String, SuperBiMessage>(), this::generateTopicName,
        maxSizeOfInternalBufferMap, minSizeOfInternalBufferMap, topicClusterMap,
        this::getTopicRootName, updateMaxCapacityMap,
        20, config.getKafkaConsumerConfig(), metricRegistry);
  }

  private PubsubLiteConsumerFactory<String, SuperBiMessage> getPubsubLiteConsumerFactory(
      Map<String, Consumer<Double>> updateMaxCapacityMap,
      MetricRegistry metricRegistry) {
    Map<String, List<String>> subscriptionConfigRefMap = config.getSubscriptionConfigRefMap();
    return new PubsubLiteConsumerFactory<String, SuperBiMessage>(
        new DefaultPollStrategy<String, SuperBiMessage>(), this::generatePubsubLiteSubscriptionName, subscriptionConfigRefMap,
        config.getPubsubLiteInfraConfigMap(),updateMaxCapacityMap,this::getTopicRootName,metricRegistry,
        config.getPrimarySecondarySubscriptionMap(),this::pubsubMessageKeyDeserializer,this::pubsubMessageValueDeserializer);
  }

  private String pubsubMessageKeyDeserializer(PubsubMessage message){
    return message.getOrderingKey();
  }

  private SuperBiMessage pubsubMessageValueDeserializer(PubsubMessage message){
    return JsonUtil.fromJson(message.getData().toStringUtf8(),SuperBiMessage.class);
  }

  private String getMetricNameForRemainingCapacity(String dataSourceId) {
    return StringUtils
        .join(Arrays.asList("compito.commandScheduler", dataSourceId, "capacity.remaining"), ".");
  }

  private String getMetricNameForRequestsInProcess(String dataSourceId) {
    return StringUtils
        .join(Arrays.asList("compito.commandScheduler", dataSourceId, "capacity.processing"), ".");
  }

  private String getMetricNameForMaxCapacity(String dataSourceId) {
    return StringUtils
        .join(Arrays.asList("compito.commandScheduler", dataSourceId, "capacity.max"), ".");
  }


  @Provides
  @Singleton
  @SneakyThrows
  private CompitoEngine providesCompitoEngine(Map<String, CommandScheduler> commandSchedulerMap) {
    return new CompitoEngine(commandSchedulerMap);
  }

  @Provides
  @Singleton
  @SneakyThrows
  private PollStrategy<String, SuperBiMessage> providesConsumerPollStrategy() {
    return new DefaultPollStrategy<>();
  }

  private String generateTopicName(String s, String s1) {
    return s.concat("-").concat(s1);
  }

  private String generatePubsubLiteSubscriptionName(String s, String s1) {
    return s.concat("-").concat(s1).concat("-sub");
  }

  private String getTopicRootName(String topicName) {
    return topicName.split("-")[0];
  }

  @Provides
  @Named("DaoProvider")
  public Map<String, DataSourceDao> getDaoProvider(@Named("DataSourceFactory")
      Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap) {
    return config.getDataSourceMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            i -> getNativeQueryExecutor(i.getValue(), dataSourceFactoryMap)));
  }

  @Provides
  @Named("AbstractDSLConfigProvider")
  public Map<String, AbstractDSLConfig> getAbstractDSLConfigProvider(@Named("DataSourceFactory")
      Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap) {
    return config.getDataSourceMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            i -> getAbstractDslConfig(i.getValue(), dataSourceFactoryMap)));
  }

  private AbstractDSLConfig getAbstractDslConfig(DataSource source,
      Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap) {
    AbstractDSLConfig dslConfig = dataSourceFactoryMap
        .get(DataSourceType.valueOf(source.getSourceType()))
        .getDslConfig(source.getDslConfig());
    if (dslConfig == null) {
      throw new RuntimeException(
          MessageFormat.format("DataSource invalid for {0} ", source.getStoreIdentifier()));
    }
    return dslConfig;
  }

  @Provides
  @Singleton
  public CosmosNativeQueryTranslator getCosmosNativeQueryTranslator(
      @Named("AbstractDSLConfigProvider") Map<String, AbstractDSLConfig> dslConfigMap,
      MetricRegistry metricRegistry) {
    return new CosmosNativeQueryTranslator(dslConfigMap, metricRegistry);
  }

  @Provides
  public CircuitBreakerRegistry getCircuitBreakerPropsProvider(
      @Named("SERVICE") MetricRegistry metricRegistry) {
    Map<String, CircuitBreakerConfig> circuitBreakerConfigMap = config.getDataSourceMap()
        .entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            i -> {
              CircuitBreakerProperties circuitBreakerProperties = i.getValue()
                  .getCircuitBreakerProperties();
              /*
              overriding slow call threshold with 24 hours irrespective of config provided,
              As in Brv2 we have our own timeouts.
              */
              circuitBreakerProperties.setSlowCallDurationThreshold(86400000L);
              return circuitBreakerProperties.getCircuitBreakerConfig();
            }));
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfigMap);
    config.getDataSourceMap().forEach((key, value) -> registry
            .circuitBreaker(value.getStoreIdentifier(), value.getStoreIdentifier()));
    CircuitBreakerMetrics.ofCircuitBreakerRegistry(registry, metricRegistry);
    return registry;
  }

  private DataSourceDao getNativeQueryExecutor(DataSource source,
      Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap) {

    DataSourceDao dataSourceDao = dataSourceFactoryMap
        .get(DataSourceType.valueOf(source.getSourceType()))
        .getDao(source.getAttributes());
    if (dataSourceDao == null) {
      throw new RuntimeException(
          MessageFormat.format("DataSource invalid for {0} ", source.getStoreIdentifier()));
    }
    return dataSourceDao;
  }
}
