
package com.flipkart.fdp.superbi.web;


import static org.asynchttpclient.Dsl.asyncHttpClient;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.audit.dao.AuditDao;
import com.flipkart.fdp.compito.api.clients.producer.Producer;
import com.flipkart.fdp.compito.api.clients.producer.ProducerCallback;
import com.flipkart.fdp.compito.api.clients.producer.ProducerRecord;
import com.flipkart.fdp.compito.api.request.Message;
import com.flipkart.fdp.compito.kafka.DefaultKafkaProducer;
import com.flipkart.fdp.compito.pubsublite.DefaultPubsubLiteProducer;
import com.flipkart.fdp.compito.pubsublite.PubsubLiteFailSafePublisher;
import com.flipkart.fdp.dao.common.STOFactory;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.util.EntityManagerProvider;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.mmg.cosmos.dao.DataSourceDao;
import com.flipkart.fdp.mmg.cosmos.dao.DimensionDao;
import com.flipkart.fdp.mmg.cosmos.dao.FactDao;
import com.flipkart.fdp.superbi.brv2.BackgroundQuerySubmitter;
import com.flipkart.fdp.superbi.brv2.ProducerErrorCallback;
import com.flipkart.fdp.superbi.brv2.SuperBiMessage;
import com.flipkart.fdp.superbi.core.adaptor.BackgroundQueryExecutorAdaptor;
import com.flipkart.fdp.superbi.core.cache.CacheKeyGenerator;
import com.flipkart.fdp.superbi.core.cache.DelegatingCacheKeyGenerator;
import com.flipkart.fdp.superbi.core.config.SuperbiConfig;
import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.sto.ReportSTOFactory;
import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClientConfiguration;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaModifier;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.JSScriptEngineAccessor;
import com.flipkart.fdp.superbi.dao.impl.CacheableDataSourceDaoImpl;
import com.flipkart.fdp.superbi.dao.impl.CacheableDimensionDaoImpl;
import com.flipkart.fdp.superbi.dao.impl.CacheableFactDaoImpl;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsConfiguration;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClient;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClientConfiguration;
import com.flipkart.fdp.superbi.http.client.qaas.QaasClient;
import com.flipkart.fdp.superbi.http.client.qaas.QaasConfiguration;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.BigTableCacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.InMemoryCacheDao;
import com.flipkart.fdp.superbi.refresher.api.execution.BackgroundRefresher;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClient;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.lock.impl.DefaultLockDaoImpl;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.fdp.superbi.web.configurations.ApplicationConfig;
import com.flipkart.fdp.superbi.web.configurations.DataSource;
import com.flipkart.fdp.superbi.web.configurations.EnvironmentConfig;
import com.flipkart.fdp.superbi.web.configurations.HealthCheckConfig;
import com.flipkart.fdp.superbi.web.configurations.SuperBiWebServiceConfiguration;
import com.flipkart.fdp.superbi.web.configurations.ext.DynamicRemoteConfig;
import com.flipkart.fdp.superbi.web.configurations.ext.LocalConfig;
import com.flipkart.fdp.superbi.web.exception.SuperBiExceptionMapper;
import com.flipkart.fdp.superbi.web.factory.ElasticSearchDataSourceFactory;
import com.flipkart.fdp.superbi.web.factory.FStreamDataSourceFactory;
import com.flipkart.fdp.superbi.web.factory.VerticaDataSourceFactory;
import com.flipkart.fdp.superbi.web.logger.AuditDBLogger;
import com.flipkart.fdp.superbi.web.logger.AuditFileLogger;
import com.flipkart.fdp.superbi.web.logger.CompositeAuditor;
import com.flipkart.fdp.superbi.web.resources.HealthCheck;
import com.flipkart.fdp.superbi.web.services.HealthCheckService;
import com.flipkart.fdp.superbi.web.util.ClassScannerUtil;
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
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.protobuf.ByteString;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.metrics.CircuitBreakerMetrics;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.models.TopicInfraConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.hibernate.SessionFactory;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.jetbrains.annotations.NotNull;
import org.threeten.bp.Duration;

/**
 * Created by : waghmode.tayappa Date : Jun 20, 2019
 */
@Slf4j
public class SuperBiWebModule extends AbstractModule {

  private final SuperBiWebServiceConfiguration configService;
  private final EnvironmentConfig environmentConfig;
  private final List<? extends Module> modules;
  private final ApplicationConfig config;


  @SneakyThrows
  SuperBiWebModule(SuperBiWebServiceConfiguration configService) {
    this.configService = configService;
    this.environmentConfig = configService.getEnvironmentConfig();
    this.modules = Lists.newArrayList(this);

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
  protected Set<Class> getClasses(final String packageName, Class... parentClasses) {
    return ClassScannerUtil.getClasses(packageName, parentClasses);
  }

  private boolean isInstantiable(Class clazz) {
    boolean isAbstract = Modifier.isAbstract(clazz.getModifiers());
    boolean isInterface = clazz.isInterface();
    return !(isAbstract || isInterface);
  }

  private void bindToImpl(Class inf, Class impl) {
    if (isInstantiable(impl) && inf.isAssignableFrom(impl)) {
      bind(inf).to(impl);
    }
  }

  @SneakyThrows
  private void bindToImpl(Class clazz) {
    Class implClass = Class.forName(
        clazz.getPackage().getName() + ".impl." + clazz.getSimpleName() + "Impl");
    bindToImpl(clazz, implClass);
  }

  @Provides
  @Named("SERVICE")
  private MetricRegistry getMetricRegistry(Injector injector) {
    return injector.getInstance(MetricRegistry.class);
  }

  @Provides
  @Named("HYDRA")
  private GetEntityManagerFunction<GenericDAO, EntityManager> getHydraEntityManagerFunction() {
    final String persistenceUnit = "hydra_read";
    Map<String, String> overrides = config.getPersistenceOverrides(persistenceUnit);

    return dao -> {
      return EntityManagerProvider.getEntityManager(persistenceUnit, overrides);
    };
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
    final String persistenceUnit = "cosmos_mmg_read";
    final String persistenceConfigKey = "cosmos_read";
    Map<String, String> overrides = config.getPersistenceOverrides(persistenceConfigKey);
    return dao -> {
      return EntityManagerProvider.getEntityManager(persistenceUnit, overrides);
    };
  }

  protected void initializeCosmos() {
//    Map overrides = Maps.newHashMap();
//    overrides.put("hibernate.current_session_context_class",
//        "org.hibernate.context.internal.ThreadLocalSessionContext");
//
//    overrides.put("hibernate.show_sql", "false");

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
  @Named("RESULT_STORE")
  private CacheDao getResultStore(CacheDao cacheDao) {
    return cacheDao;
  }

  @Provides
  @Named("ATTEMPT_STORE")
  private CacheDao getAttemptStore(CacheDao cacheDao) {
    return cacheDao;
  }

  @Provides
  @Named("HANDLE_STORE")
  private CacheDao getHandleStore(CacheDao cacheDao) {
    return cacheDao;
  }


  @Override
  @SneakyThrows
  protected void configure() {
    try {
      bind(SuperBiWebServiceConfiguration.class).toInstance(configService);
      bind(HealthCheckConfig.class).toInstance(configService.getHealthCheckConfig());
      bind(EnvironmentConfig.class).toInstance(environmentConfig);
      bind(HealthCheckService.class).asEagerSingleton();

      // ResourceClases
      Set<Class> resourceClasses = getClasses(HealthCheck.class.getPackage().getName(),
          Object.class);
      bindInScope(resourceClasses, Singleton.class);

      Consumer<Class> bindToImpl = (clazz) -> {
        if (clazz.isInterface()) {
          bindToImpl(clazz);
        }
      };

      // Dao interface and impls mapping
      Set<Class> daoClasses = getClasses("com.flipkart.fdp.superbi.dao", GenericDAO.class);
      daoClasses.forEach(bindToImpl);

      // STOFactory interface and impls mapping
      Set<Class> stoFactoryClasses = getClasses(ReportSTOFactory.class.getPackage().getName(),
          STOFactory.class);
      stoFactoryClasses.forEach(bindToImpl);

      /**
       * Bind COSMOS dao from cosmos-db dependency
       * We do not want dependency on COSMOS lib here, and dont want API call with MMG.
       * Hence taking mmg module to talk to COSMOS db
       */

      Set<Class> cosmosDaos = getClasses(FactDao.class.getPackage().getName(),
          GenericDAO.class);
      cosmosDaos.removeAll(Lists.newArrayList(FactDao.class, DimensionDao.class, DataSourceDao.class));
      cosmosDaos.forEach(bindToImpl);
      // added custom binding for following entities
      bind(FactDao.class).to(CacheableFactDaoImpl.class);
      bind(DimensionDao.class).to(CacheableDimensionDaoImpl.class);
      bind(DataSourceDao.class).to(CacheableDataSourceDaoImpl.class);

      /**
       * Bind Audit Dao from audit-db dependency
       */
      Set<Class> auditDaos = getClasses(AuditDao.class.getPackage().getName(),
          GenericDAO.class);
      auditDaos.forEach(bindToImpl);

      bind(SuperbiConfig.class).toInstance(config);
      bind(ApplicationConfig.class).toInstance(config);
      bind(CacheKeyGenerator.class).to(DelegatingCacheKeyGenerator.class).in(Singleton.class);

      SuperBiExceptionMapper superBiExceptionMapper = new SuperBiExceptionMapper(
          () -> config.getExceptionInfoMap());
      bind(SuperBiExceptionMapper.class).toInstance(superBiExceptionMapper);

      bind(AuditDBLogger.class).in(Singleton.class);
      bind(AuditFileLogger.class).in(Singleton.class);

      initializeCosmos();

      JSScriptEngineAccessor.initScriptEngine(config.getConcurrencyForScriptEngine());
      com.flipkart.fdp.superbi.dsl.evaluators.JSScriptEngineAccessor.initScriptEngine(
          config.getConcurrencyForScriptEngine());

      //Hydra_read
      Map<String, String> overrides_hr = config.getPersistenceOverrides("hydra_read");
      EntityManagerProvider.getEntityManager("hydra_read", overrides_hr);
      log.info("Hydra Read EMF built");

      //MMG read
      Map<String, String> overrides_cr = config.getPersistenceOverrides("cosmos_read");
      EntityManagerProvider.getEntityManager("cosmos_mmg_read", overrides_cr);
      log.info("MMG Cosmos Read EMF built");

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
  @Named("AUDITERS")
  private List<Auditer> getAuditersList(AuditDBLogger auditDBLogger,
      AuditFileLogger auditFileLogger) {
    return Lists.newArrayList(auditDBLogger, auditFileLogger);
  }

  @NotNull
  @Provides
  @Singleton
  private Auditer getAuditer(CompositeAuditor compositeAuditor) {
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
  private BackgroundQueryExecutorAdaptor providesBackgroundQueryExecutorAdaptor(
      BackgroundRefresher backgroundRefresher,
      @Named("AbstractDSLConfigProvider") Map<String, AbstractDSLConfig> dslConfigMap,
      MetricRegistry metricRegistry) {
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForMmg(MmgClient.SERVICE_NAME
        , config.getMmgClientConfiguration(), metricRegistry);
    MmgClient mmgClient = new MmgClient(asyncHttpClient, config.getMmgClientConfiguration());
    BadgerClient badgerClient = prepareBadgerClient(metricRegistry);
    BadgerClient.setInstance(badgerClient);
    return new BackgroundQueryExecutorAdaptor(backgroundRefresher, dslConfigMap, metricRegistry,
        config::checkDSQuerySerialization, config::shouldCalculatePriority, mmgClient,
        config::getElasticSearchCostBoost, config::getFactRefreshTimeRequired, badgerClient);
  }

  private AsyncHttpClient createAsyncHttpClientForMmg(String name,
      MmgClientConfiguration mmgClientConfiguration, MetricRegistry metricRegistry) {
    return ResilientDomain.builder(name)
        .circuitBreakerConfig(
            mmgClientConfiguration.getCircuitBreakerProperties().getCircuitBreakerConfig())
        .bulkheadConfig(mmgClientConfiguration.getCircuitBreakerProperties().getBulkheadConfig())
        .asyncHttpClient(asyncHttpClient(
            Dsl.config().setMaxConnections(mmgClientConfiguration.getMaxConnectionsTotal())
                .setMaxConnectionsPerHost(mmgClientConfiguration.getMaxConnectionPerHost())
                .setPooledConnectionIdleTimeout(
                    mmgClientConfiguration.getPooledConnectionIdleTimeout())
                .setConnectionTtl(mmgClientConfiguration.getConnectionTtlInMillis())
                .setConnectTimeout(mmgClientConfiguration.getConnectTimeout())
                .setRequestTimeout(mmgClientConfiguration.getRequestTimeoutInMillies())
                .build()))
        .metricRegistry(metricRegistry).build();
  }

  @Provides
  @Singleton
  private org.apache.kafka.clients.producer.Producer<String, SuperBiMessage> providesKafkaProducer() {
    return new KafkaProducer<>(config.getKafkaProducerConfig());
  }


  @NotNull
  @Provides
  @Singleton
  private ProducerCallback<String, SuperBiMessage> providesProducerCallback() {
    return new ProducerErrorCallback();
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
    Map<String, String> topicToInfraConfigRefMap = config.getTopicToInfraConfigRefMap();
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
    if (config.getMessageQueue() == MessageQueue.PUBSUB_LITE) {
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
  private BackgroundRefresher providesBackgroundRefresher(
      Producer<String, SuperBiMessage> producer,
      ProducerCallback<String, SuperBiMessage> producerCallback) {
    return new BackgroundQuerySubmitter(producer, producerCallback,
        (storeIdentifier) -> {
          if (!config.getDataSourceMap().containsKey(storeIdentifier)) {
            throw new UnsupportedOperationException(
                MessageFormat.format("StoreIdentifier {0} is not supported", storeIdentifier));
          }
          return config.getBackgroundRefresherConfig(storeIdentifier);
        },
        this::generateTopicName, config.getMessageQueue());
  }

  private String generateTopicName(QueryPayload queryPayload) {
    return queryPayload.getStoreIdentifier().concat("-").concat(queryPayload.getPriority());
  }


  @Provides
  @Named("D42UploadClients")
  private List<String> getD42UploadClients() {
    return config.getD42MetaConfig().getD42UploadClients();
  }

  @Provides
  @Singleton
  @SneakyThrows
  private CacheDao cacheDaoProvider(MetricRegistry metricRegistry) {
    if("local".equals(this.environmentConfig.getEnv())) {
      return new InMemoryCacheDao(1000,30, TimeUnit.DAYS);
    }
    return new BigTableCacheDao(config.getResultTableClient(), metricRegistry);
//    } else {
//      Bucket resultStoreBucket = config.getResultStoreBucketConfig().getCouchbaseBucket();
//      return new CouchbaseCacheDao(resultStoreBucket, metricRegistry);
//    }
  }

  @Provides
  @Singleton
  @SneakyThrows
  private LockDao lockDaoProvider(MetricRegistry metricRegistry) {
    CacheDao cacheDao = null;
    if("local".equals(this.environmentConfig.getEnv())) {
      cacheDao = new InMemoryCacheDao(1000,30, TimeUnit.DAYS);
    } else {
      cacheDao = new BigTableCacheDao(config.getResultTableClient(), metricRegistry);
    }
//    } else {
//      cacheDao = new CouchbaseCacheDao(
//          config.getLockStoreBucketConfig().getCouchbaseBucket(), metricRegistry);
//    }
    return new DefaultLockDaoImpl(cacheDao);
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
  public FStreamClient prepareFStreamClient(MetricRegistry metricRegistry) {
    FStreamClientConfiguration configuration = config.getFStreamClientConfiguration();
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForFStream(FStreamClient.SERVICE_NAME,
        config.getFStreamClientConfiguration(),
        metricRegistry);
    return new FStreamClient(asyncHttpClient, configuration.getBasePath(), configuration.getPort());
  }

  @Provides
  @Singleton
  public BadgerClient prepareBadgerClient(MetricRegistry metricRegistry) {
    BadgerClientConfiguration badgerConfiguration = config.getBadgerClientConfiguration();
    AsyncHttpClient asyncHttpClient = createAsyncHttpClientForBadger(BadgerClient.SERVICE_NAME,
        config.getBadgerClientConfiguration(),
        metricRegistry);
    return new BadgerClient(asyncHttpClient, badgerConfiguration.getBasePath(), badgerConfiguration.getPort());
  }

  @Provides
  @Singleton
  public QaasClient prepareQaasDownloader() {
    QaasConfiguration configuration = config.getQaasConfiguration();
    return new QaasClient(configuration);
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

  private AsyncHttpClient createAsyncHttpClientForBadger(String serviceName,
                                                          BadgerClientConfiguration badgerClientConfiguration, MetricRegistry metricRegistry) {
    return ResilientDomain.builder(serviceName)
            .circuitBreakerConfig(
                    badgerClientConfiguration.getCircuitBreakerProperties().getCircuitBreakerConfig())
            .bulkheadConfig(
                    badgerClientConfiguration.getCircuitBreakerProperties().getBulkheadConfig())
            .asyncHttpClient(asyncHttpClient(
                    Dsl.config().setMaxConnections(badgerClientConfiguration.getMaxConnectionsTotal())
                            .setMaxConnectionsPerHost(badgerClientConfiguration.getMaxConnectionPerHost())
                            .setPooledConnectionIdleTimeout(
                                    badgerClientConfiguration.getPooledConnectionIdleTimeout())
                            .setConnectionTtl(badgerClientConfiguration.getConnectionTtlInMillis())
                            .setConnectTimeout(badgerClientConfiguration.getConnectTimeout())
                            .setRequestTimeout(badgerClientConfiguration.getRequestTimeoutInMillies())
                            .build()))
            .metricRegistry(metricRegistry).build();
  }

  @Provides
  @Named("AbstractDSLConfigProvider")
  public Map<String, AbstractDSLConfig> getAbstractDSLConfigProvider() {
    return config.getDataSourceMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            i -> getAbstractDslConfig(i.getValue())));
  }

  @Provides
  @Named("SourceLimitProvider")
  public Map<String, Integer> getSourceDataLimitMap() {
    return config.getDataSourceMap().entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, i -> i.getValue().getLimit()));
  }

  private AbstractDSLConfig getAbstractDslConfig(DataSource source) {
    try {
      return DataSourceType.valueOf(source.getSourceType())
          .getDslConfig(source.getDslConfig());
    }catch (IllegalArgumentException exception) {
      throw new RuntimeException(
          MessageFormat.format("DataSource invalid for {0} ", source.getStoreIdentifier()));
    }
  }

  @Provides
  public CircuitBreakerRegistry getCircuitBreakerPropsProvider(
      @Named("SERVICE") MetricRegistry metricRegistry) {
    Map<String, CircuitBreakerConfig> circuitBreakerConfigMap = config.getDataSourceMap()
        .entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            i -> i.getValue().getCircuitBreakerProperties().getCircuitBreakerConfig()));
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfigMap);
    config.getDataSourceMap()
        .entrySet().forEach(i -> registry
        .circuitBreaker(i.getValue().getStoreIdentifier(), i.getValue().getStoreIdentifier()));
    CircuitBreakerMetrics.ofCircuitBreakerRegistry(registry, metricRegistry);
    return registry;
  }

  List<? extends Module> getModules() {
    return modules;
  }
}
