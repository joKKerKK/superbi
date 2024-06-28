package com.flipkart.fdp.superbi.subscription.app;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.dao.common.dao.jpa.GenericDAO;
import com.flipkart.fdp.dao.common.util.EntityManagerProvider;
import com.flipkart.fdp.dao.common.util.GetEntityManagerFunction;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.d42.D42Uploader;
import com.flipkart.fdp.superbi.dao.SubscriptionEventDao;
import com.flipkart.fdp.superbi.gcs.GcsConfig;
import com.flipkart.fdp.superbi.gcs.GcsUploader;
import com.flipkart.fdp.superbi.mail.DefaultEmailClient;
import com.flipkart.fdp.superbi.mail.EmailClient;
import com.flipkart.fdp.superbi.subscription.client.ClientFactory;
import com.flipkart.fdp.superbi.subscription.client.PlatoExecutionClient;
import com.flipkart.fdp.superbi.subscription.client.PlatoMetaClient;
import com.flipkart.fdp.superbi.subscription.client.SuperBiClient;
import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.subscription.configurations.*;
import com.flipkart.fdp.superbi.subscription.configurations.ext.DynamicRemoteConfig;
import com.flipkart.fdp.superbi.subscription.configurations.ext.LocalConfig;
import com.flipkart.fdp.superbi.subscription.configurations.quartz.QuartzJobFactory;
import com.flipkart.fdp.superbi.subscription.delivery.D42Util;
import com.flipkart.fdp.superbi.subscription.delivery.DeliveryExecutor;
import com.flipkart.fdp.superbi.subscription.delivery.EmailExecutor;
import com.flipkart.fdp.superbi.subscription.delivery.FtpExecutor;
import com.flipkart.fdp.superbi.subscription.delivery.GsheetUtil;
import com.flipkart.fdp.superbi.subscription.delivery.SftpExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLoggerImpl;
import com.flipkart.fdp.superbi.subscription.executors.DefaultRetryJobHandler;
import com.flipkart.fdp.superbi.subscription.executors.RetryJobHandler;
import com.flipkart.fdp.superbi.subscription.executors.RetrySubscriptionJob;
import com.flipkart.fdp.superbi.subscription.executors.SubscriptionJob;
import com.flipkart.fdp.superbi.subscription.model.DeliveryData.DeliveryAction;
import com.flipkart.fdp.superbi.subscription.resources.HealthCheck;
import com.flipkart.fdp.superbi.subscription.util.ClassScannerUtil;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import emailsvc.CommunicationService;
import emailsvc.Connekt.ConnektCommunicationService;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.metrics.BulkheadMetrics;
import io.github.resilience4j.metrics.CircuitBreakerMetrics;
import io.github.resilience4j.metrics.RetryMetrics;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.persistence.EntityManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;

@Slf4j
public class SubscriptionServiceModule extends AbstractModule {
  private final SubscriptionServiceConfiguration configService;
  private final EnvironmentConfig environmentConfig;
  private final SubscriptionConfig config;

  @SneakyThrows
  public SubscriptionServiceModule(SubscriptionServiceConfiguration configService) {
    this.configService = configService;
    this.environmentConfig = configService.getEnvironmentConfig();


    SubscriptionConfig subscriptionConfig = DynamicRemoteConfig.getInstance();
    if ("local".equals(this.environmentConfig.getEnv())) {
      this.config = new LocalConfig(subscriptionConfig);
    } else {
      this.config = subscriptionConfig;
    }
  }

  @Provides
  @Named("SERVICE")
  private MetricRegistry getMetricRegistry(Injector injector) {
    return injector.getInstance(MetricRegistry.class);
  }

  @Override
  protected void configure() {
    try {
      bind(HealthCheckConfig.class).toInstance(configService.getHealthCheckConfig());
      bind(SubscriptionServiceConfiguration.class).toInstance(configService);
      bind(EnvironmentConfig.class).toInstance(configService.getEnvironmentConfig());
      bind(SubscriptionConfig.class).toInstance(config);

      Set<Class> resourceClasses = getClasses(HealthCheck.class.getPackage().getName(),
          Object.class);
      bindInScope(resourceClasses, Singleton.class);

      Consumer<Class> bindToImpl = (clazz) -> {
        if (clazz.isInterface()) {
          bindToImpl(clazz);
        }
      };

      Set<Class> subscriptionDaos = getClasses(SubscriptionEventDao.class.getPackage().getName(),
          GenericDAO.class);
      subscriptionDaos.forEach(bindToImpl);

      Map<String, String> overrides_hydra = config.getPersistenceOverrides("hydra");
      EntityManagerProvider.getEntityManager("hydra", overrides_hydra);
      log.info("Hydra DB EMF built");




     } catch (Exception ex) {
      log.error("Error while initialising guice :", ex);
      throw ex;
    }

  }

  @Provides
  @Singleton
  private SubscriptionEventLogger getSubscriptionEventLogger(SubscriptionEventDao subscriptionEventDao){
    return new SubscriptionEventLoggerImpl(subscriptionEventDao, config);
  }

  @Provides
  @Singleton
  private SuperBiClient getSuperbiCallClient(MetricRegistry metricRegistry){
    CircuitBreakerRegistry circuitBreakerRegistry = getCircuitBreakerRegistry(metricRegistry, config.getSuperbiClientConfig()
            .getCircuitBreakerProperties().getCircuitBreakerConfig(), SuperBiClient.class.getName());
    BulkheadRegistry bulkheadRegistry = getBulkHeadRegistry(metricRegistry, config.getSuperbiClientConfig().getCircuitBreakerProperties().getBulkheadConfig(),
            SuperBiClient.class.getName());
    return new SuperBiClient(ClientFactory.getSuperbiClient(
        config.getSuperbiClientConfig()),circuitBreakerRegistry,bulkheadRegistry,metricRegistry,
        config.getSuperbiClientConfig());

  }

  @Provides
  @Singleton
  private PlatoMetaClient providesPlatoMetaClient(MetricRegistry metricRegistry) {
    CircuitBreakerRegistry circuitBreakerRegistry = getCircuitBreakerRegistry(metricRegistry, config.getPlatoMetaClientConfig().getCircuitBreakerProperties().getCircuitBreakerConfig(),
            PlatoMetaClient.class.getName());
    BulkheadRegistry bulkheadRegistry = getBulkHeadRegistry(metricRegistry, config.getPlatoMetaClientConfig().getCircuitBreakerProperties().getBulkheadConfig(),
            PlatoMetaClient.class.getName());
    return new PlatoMetaClient(ClientFactory.getPlatoMetaClient(
            config.getPlatoMetaClientConfig()),circuitBreakerRegistry,bulkheadRegistry,metricRegistry,
            config.getPlatoMetaClientConfig());

  }

  @Provides
  @Singleton
  private PlatoExecutionClient providesPlatoExecutionClient(MetricRegistry metricRegistry) {
    CircuitBreakerRegistry circuitBreakerRegistry = getCircuitBreakerRegistry(metricRegistry, config.getPlatoExecutionClientConfig().getCircuitBreakerProperties().getCircuitBreakerConfig(),
            PlatoExecutionClient.class.getName());
    BulkheadRegistry bulkheadRegistry = getBulkHeadRegistry(metricRegistry, config.getPlatoExecutionClientConfig().getCircuitBreakerProperties().getBulkheadConfig(),
            PlatoExecutionClient.class.getName());
    return new PlatoExecutionClient(ClientFactory.getPlatoMetaClient(
            config.getPlatoMetaClientConfig()),circuitBreakerRegistry,bulkheadRegistry,metricRegistry,
            config.getPlatoExecutionClientConfig());

  }

  private BulkheadRegistry getBulkHeadRegistry(MetricRegistry metricRegistry, BulkheadConfig bulkheadConfig, String className) {
//    BulkheadConfig bulkheadConfig = config.getSuperbiClientConfig().getCircuitBreakerProperties().getBulkheadConfig();
    BulkheadRegistry registry = BulkheadRegistry.of(bulkheadConfig);
    registry.bulkhead(className);
    BulkheadMetrics.ofBulkheadRegistry(registry,metricRegistry);
    return registry;
  }

  private CircuitBreakerRegistry getCircuitBreakerRegistry(MetricRegistry metricRegistry, CircuitBreakerConfig circuitBreakerConfig, String className) {
//    CircuitBreakerConfig circuitBreakerConfig = config.getPlatoMetaClientConfig().getCircuitBreakerProperties().getCircuitBreakerConfig();
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    registry.circuitBreaker(className);
    CircuitBreakerMetrics.ofCircuitBreakerRegistry(registry,metricRegistry);
    return registry;
  }

  @Provides
  @Singleton
  private D42Util providesD42Util(D42Uploader d42Uploader, GcsUploader gcsUploader){
    return new D42Util(d42Uploader, gcsUploader);
  }

  @Provides
  @Singleton
  private GsheetUtil providesGsheetUtil(MetricRegistry metricRegistry) throws IOException,
      GeneralSecurityException {
    return new GsheetUtil(config.getGsheetConfig(),
        getCircuitBreakerForGsheetUpload(metricRegistry), getRetryForGsheetUpload(metricRegistry),
        metricRegistry);
  }

  @Provides
  @Singleton
  private EmailExecutor providesEmailExecutor(EmailClient emailClient,D42Util d42Util, GsheetUtil gsheetUtil
      ,MetricRegistry metricRegistry,SubscriptionEventLogger subscriptionEventLogger){
    return new EmailExecutor(emailClient,config.getD42ExpiryInSeconds(),d42Util,gsheetUtil,
        metricRegistry, subscriptionEventLogger, config.getGsheetConfig());
  }

  @Provides
  @Singleton
  private FtpExecutor providesFtpExecutor(D42Client d42Client, MetricRegistry metricRegistry, SubscriptionEventLogger subscriptionEventLogger){
    return new FtpExecutor(d42Client, metricRegistry, subscriptionEventLogger);
  }

  @Provides
  @Singleton
  private SftpExecutor providesSftpExecutor(D42Client d42Client, MetricRegistry metricRegistry, SubscriptionEventLogger subscriptionEventLogger){
    return new SftpExecutor(d42Client, metricRegistry, subscriptionEventLogger);
  }



  @Provides
  private Map<DeliveryAction,DeliveryExecutor> getDeliveryExecutorMap(EmailExecutor emailExecutor,FtpExecutor ftpExecutor, SftpExecutor sftpExecutor){
    Map<DeliveryAction,DeliveryExecutor> deliveryExecutorMap = new HashMap<>();
    deliveryExecutorMap.put(DeliveryAction.EMAIL, emailExecutor);
    deliveryExecutorMap.put(DeliveryAction.FTP,ftpExecutor);
    deliveryExecutorMap.put(DeliveryAction.SFTP,sftpExecutor);
    return deliveryExecutorMap;
  }

  @Provides
  @Singleton
  private SubscriptionJob providesSubscriptionJob(SuperBiClient superBiClient,Map<DeliveryAction,
      DeliveryExecutor> deliveryExecutorMap,RetryJobHandler retryJobHandler,
      MetricRegistry metricRegistry,SubscriptionEventLogger subscriptionEventLogger, PlatoMetaClient platoMetaClient, PlatoExecutionClient platoExecutionClient){
    return new SubscriptionJob(superBiClient,deliveryExecutorMap
        ,retryJobHandler,metricRegistry,subscriptionEventLogger, config.getMaxSubscriptionRunsLeftForComm(), config.getMaxDaysLeftForComm(), platoExecutionClient, platoMetaClient);
  }

  @Provides
  @Singleton
  private RetrySubscriptionJob providesRetrySubscriptionJob(SuperBiClient superBiClient,Map<DeliveryAction,
      DeliveryExecutor> deliveryExecutorMap, MetricRegistry metricRegistry
      ,SubscriptionEventLogger subscriptionEventLogger, PlatoMetaClient platoMetaClient, PlatoExecutionClient platoExecutionClient){
    return new RetrySubscriptionJob(superBiClient,deliveryExecutorMap
    ,metricRegistry,subscriptionEventLogger, config.getMaxSubscriptionRunsLeftForComm(), config.getMaxDaysLeftForComm(), platoExecutionClient, platoMetaClient);
  }

  @Provides
  @Singleton
  private RetryJobHandler providesRetryJobHandler(Scheduler scheduler){
    return new DefaultRetryJobHandler(scheduler,config.getSubscriptionJobConfig().getBackOffTimeInMillis());
  }


  @SneakyThrows
  @Provides
  @Singleton
  private Scheduler providesScheduler(Injector injector){
    Scheduler scheduler = new StdSchedulerFactory(config.getSchedulerConfig().buildProps()).getScheduler();
    scheduler.setJobFactory(injector.getInstance(QuartzJobFactory.class));
    scheduler.addJob(constructJobDetailForQuartz(),true);
    return scheduler;
  }

  private JobDetail constructJobDetailForQuartz(){
    return JobBuilder
        .newJob(SubscriptionJob.class)
        .withIdentity("subscription")
        .storeDurably(true)
        .build();
  }

  @NotNull
  @Provides
  @Singleton
  public D42Client d42ClientProvider() {
    D42Configuration d42Configuration = config.getD42Configuration();

    return new D42Client(d42Configuration.getD42Endpoint(),d42Configuration.getAccessKey(),
        d42Configuration.getSecretKey(),d42Configuration.getBucket(),
        d42Configuration.getProjectId());
  }

  @NotNull
  @Provides
  @Singleton
  public GcsUploader gcsUploaderProvider(MetricRegistry metricRegistry) {
    GcsConfig gcsConfig = config.getGcsConfig();

    return new GcsUploader(gcsConfig, metricRegistry);
  }

  private AmazonS3 getD42Connection(){
    D42Configuration d42Configuration = config.getD42Configuration();
    AWSCredentials credentials = new BasicAWSCredentials(d42Configuration.getAccessKey(), d42Configuration.getSecretKey());
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setProtocol(Protocol.HTTP);
    AmazonS3 conn = new AmazonS3Client(credentials, clientConfig);
    conn.setEndpoint(d42Configuration.getD42Endpoint());
    return conn;
  }

  @Provides
  @Singleton
  public CommunicationService providesCommunicationService(){
    return new ConnektCommunicationService(config.getConnektServiceConfig());
  }

  @Provides
  @Named("HYDRA")
  @Singleton
  private GetEntityManagerFunction<GenericDAO, EntityManager> getHydraEntityManagerFunction() {
    final String persistenceUnit = "hydra";
    Map<String, String> overrides = config.getPersistenceOverrides(persistenceUnit);

    return dao -> {
      return EntityManagerProvider.getEntityManager(persistenceUnit, overrides);
    };
  }

  @Provides
  @Singleton
  public EmailClient providesEmailClient(){

    DefaultEmailConfig defaultEmailConfig = config.getDefaultEmailClientConfig();
    Authenticator auth = new Authenticator() {
      //override the getPasswordAuthentication method
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(defaultEmailConfig.getUserName(),
            defaultEmailConfig.getPassword());
      }
    };
    Properties props = buildPropertiesForMailSession(defaultEmailConfig);
    Session session =  Session.getInstance(props, auth);

    return new DefaultEmailClient(session,config.getEmailTemplate(),
        config.getGsheetCreationEmailTemplate(), config.getGsheetCancelledEmailTemplate(),
        config.getGsheetOverwriteEmailTemplate(),
        config.getFailureEmailTemplate(), config.getExpirationCommTemplate());
  }

  @NotNull
  private Properties buildPropertiesForMailSession(DefaultEmailConfig defaultEmailConfig) {
    Properties props = System.getProperties();
    props.put("mail.smtp.auth", true);
    props.put("mail.smtp.starttls.enable", "false");
    props.put("mail.user", defaultEmailConfig.getUserName());
    props.put("mail.password", defaultEmailConfig.getPassword() );
    props.put("mail.smtp.host", defaultEmailConfig.getHost());
    return props;
  }

  @Provides
  @Singleton
  public D42Uploader provideD42Uploader(D42Client d42Client,MetricRegistry metricRegistry) {

    return new D42Uploader(d42Client, getCircuitBreakerForD42Uploader(metricRegistry),getRetryForD42Uploader(metricRegistry), metricRegistry);
  }

  private Retry getRetryForD42Uploader(MetricRegistry metricRegistry) {
    RetryConfig retryConfig = config.getD42Configuration()
        .getCircuitBreakerProperties().getRetryConfig();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    Retry retry = registry.retry(D42Uploader.class.getName());
    RetryMetrics.ofRetryRegistry(registry,metricRegistry);
    return retry;
  }

  private CircuitBreaker getCircuitBreakerForD42Uploader(MetricRegistry metricRegistry) {
    CircuitBreakerConfig circuitBreakerConfig = config.getD42Configuration()
        .getCircuitBreakerProperties().getCircuitBreakerConfig();
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    CircuitBreaker circuitBreaker = registry.circuitBreaker(D42Uploader.class.getName());
    CircuitBreakerMetrics.ofCircuitBreakerRegistry(registry,metricRegistry);
    return circuitBreaker;
  }

  private Retry getRetryForGsheetUpload(MetricRegistry metricRegistry) {
    RetryConfig retryConfig = config.getGsheetConfig()
        .getCircuitBreakerProperties().getRetryConfig();
    RetryRegistry registry = RetryRegistry.of(retryConfig);
    Retry retry = registry.retry(GsheetUtil.class.getName());
    RetryMetrics.ofRetryRegistry(registry,metricRegistry);
    return retry;
  }

  private CircuitBreaker getCircuitBreakerForGsheetUpload(MetricRegistry metricRegistry) {
    CircuitBreakerConfig circuitBreakerConfig = config.getGsheetConfig()
        .getCircuitBreakerProperties().getCircuitBreakerConfig();
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    CircuitBreaker circuitBreaker = registry.circuitBreaker(GsheetUtil.class.getName());
    CircuitBreakerMetrics.ofCircuitBreakerRegistry(registry,metricRegistry);
    return circuitBreaker;
  }

  @SneakyThrows
  protected Set<Class> getClasses(final String packageName, Class... parentClasses) {
    return ClassScannerUtil.getClasses(packageName, parentClasses);
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


}
