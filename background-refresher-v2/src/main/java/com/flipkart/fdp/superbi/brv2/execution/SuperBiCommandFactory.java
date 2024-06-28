package com.flipkart.fdp.superbi.brv2.execution;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.compito.api.cache.DedupeStore;
import com.flipkart.fdp.compito.api.clients.RecordConverter;
import com.flipkart.fdp.compito.api.clients.consumer.ConsumerRecord;
import com.flipkart.fdp.compito.api.clients.producer.Producer;
import com.flipkart.fdp.compito.api.clients.producer.ProducerCallback;
import com.flipkart.fdp.compito.api.command.Command;
import com.flipkart.fdp.compito.api.command.CommandFactory;
import com.flipkart.fdp.compito.api.retry.RetryConfig;
import com.flipkart.fdp.compito.api.retry.RetryableCommand;
import com.flipkart.fdp.superbi.brv2.SuperBiMessage;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.execution.BackgroundRefreshTaskExecutor;
import com.flipkart.fdp.superbi.execution.RetryTaskHandler;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.inject.Inject;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.stat.Statistics;

public class SuperBiCommandFactory implements CommandFactory<String, SuperBiMessage, QueryResult> {

  private static final String METRICS_PREFIX = "compito.commandScheduler";
  private final DedupeStore<String, SuperBiMessage> dedupeStore;
  private final Function<String, Long> ttlAfterSuccessProvider;
  private final Producer<String, SuperBiMessage> producer;
  private final ProducerCallback<String, SuperBiMessage> producerCallback;
  private final RecordConverter<String, SuperBiMessage> converter;
  private final BackgroundRefreshTaskExecutor taskExecutor;
  private final Function<String, BackgroundRefresherConfig> backgroundRefresherConfigProvider;
  private final Function<String, RetryConfig> retryConfigProvider;
  private final LockDao lockDao;
  private final RetryTaskHandler retryTaskHandler;
  private final MetricRegistry metricRegistry;
  private static final String BRV2_COSMOS_READ_METRIC = "hibernate.brv2.cosmos_read";

  @Inject
  public SuperBiCommandFactory(
      DedupeStore<String, SuperBiMessage> dedupeStore,
      Function<String, Long> ttlAfterSuccessProvider,
      Producer<String, SuperBiMessage> producer,
      ProducerCallback<String, SuperBiMessage> producerCallback,
      RecordConverter<String, SuperBiMessage> converter,
      BackgroundRefreshTaskExecutor taskExecutor,
      Function<String, BackgroundRefresherConfig> backgroundRefresherConfigProvider,
      Function<String, RetryConfig> retryConfigProvider,
      LockDao lockDao, RetryTaskHandler retryTaskHandler, MetricRegistry metricRegistry) {
    this.dedupeStore = dedupeStore;
    this.ttlAfterSuccessProvider = ttlAfterSuccessProvider;
    this.producer = producer;
    this.producerCallback = producerCallback;
    this.converter = converter;
    this.taskExecutor = taskExecutor;
    this.backgroundRefresherConfigProvider = backgroundRefresherConfigProvider;
    this.retryConfigProvider = retryConfigProvider;
    this.lockDao = lockDao;
    this.retryTaskHandler = retryTaskHandler;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public Command<String, SuperBiMessage, QueryResult> create(
      ConsumerRecord<String, SuperBiMessage> consumerRecord) {
    String storeIdentifier = consumerRecord.value().getQueryPayload()
        .getStoreIdentifier();
    BackgroundRefresherConfig backgroundRefresherConfig = backgroundRefresherConfigProvider
        .apply(storeIdentifier);
    RetryConfig retryConfig = retryConfigProvider.apply(storeIdentifier);
    long ttlAfterSuccessInMillis = ttlAfterSuccessProvider.apply(storeIdentifier);
    SuperBiCommand command = new SuperBiCommand(consumerRecord, taskExecutor,
        backgroundRefresherConfig, lockDao, retryTaskHandler, metricRegistry);

    try {
      return new RetryableCommand<>(dedupeStore, ttlAfterSuccessInMillis, command, producer,
          producerCallback, converter, retryConfig,
          metricRegistry.meter(getMetricKeyForRetry(storeIdentifier)),
          metricRegistry.meter(getMetricKeyForRetryExhausted(storeIdentifier)));
    } finally {
      // Pushing metrics for cosmos_read
      Statistics cosmosReadStatistics = MetaAccessor.get().getSessionFactory().getStatistics();
      registerHibernateMetrics(metricRegistry, cosmosReadStatistics, BRV2_COSMOS_READ_METRIC);
    }
  }

  private void registerHibernateMetrics(MetricRegistry metricRegistry, Statistics statistics, String prefix) {
    metricRegistry.gauge(prefix + ".cache.hit", () -> statistics::getQueryCacheHitCount);
    metricRegistry.gauge(prefix + ".cache.miss", () -> statistics::getQueryCacheMissCount);
    metricRegistry.gauge(prefix + ".connections", () -> statistics::getConnectCount);
    metricRegistry.gauge(prefix + ".entity_count", () -> statistics::getEntityFetchCount);
  }

  private String getMetricKeyForRetryExhausted(String storeIdentifier) {
    return StringUtils.join(Arrays.asList(METRICS_PREFIX, storeIdentifier, "retryExhausted"), ".");
  }

  private String getMetricKeyForRetry(String storeIdentifier) {
    return StringUtils.join(Arrays.asList(METRICS_PREFIX, storeIdentifier, "retry"), ".");
  }
}
