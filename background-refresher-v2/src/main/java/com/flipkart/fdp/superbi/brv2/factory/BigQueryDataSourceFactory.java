package com.flipkart.fdp.superbi.brv2.factory;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.BigQueryClientConfig;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.BigQueryDataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.BigQueryJobConfig;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.executor.BigQueryExecutorImpl;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import java.util.Map;

import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;

@AllArgsConstructor
@Slf4j
public class BigQueryDataSourceFactory implements DataSourceFactory {

  private final CacheDao jobStore;
  private final GringottsClient gringottsClient;
  private final MetricRegistry metricRegistry;

  @Override
  @SneakyThrows
  public DataSourceDao getDao(Map<String, Object> attributes) {
    BigQueryJobConfig bigQueryJobConfig = getBigQueryJobConfig(attributes);

    BigQueryClientConfig bigQueryClientConfig = getBigQueryClientConfig(
        attributes);

    Credentials credentials = ServiceAccountCredentials.getApplicationDefault();
    BigQueryOptions.Builder bigQueryOptionsBuilder = BigQueryOptions.newBuilder()
            .setProjectId(bigQueryClientConfig.getProjectId())
            .setCredentials(credentials);

    if (bigQueryClientConfig.getClientRetryConfig().isPresent()) {
      log.info(String.format("Adding following retry config to BQ client: ", bigQueryClientConfig.getClientRetryConfig().get()));
      RetrySettings.Builder retrySettings = RetrySettings.newBuilder();
      BigQueryClientConfig.RetryConfig retryConfig = bigQueryClientConfig.getClientRetryConfig().get();
      if (retryConfig.getMaxAttempts().isPresent()) {
        retrySettings.setMaxAttempts(retryConfig.getMaxAttempts().get());
      }
      if (retryConfig.getInitialRetryDelayMs().isPresent()) {
        retrySettings.setInitialRetryDelay(Duration.ofMillis(retryConfig.getInitialRetryDelayMs().get()));
      }
      if (retryConfig.getRetryDelayMultiplier().isPresent()) {
        retrySettings.setRetryDelayMultiplier(retryConfig.getRetryDelayMultiplier().get());
      }
      if (retryConfig.getMaxRetryDelayMs().isPresent()) {
        retrySettings.setMaxRetryDelay(Duration.ofMillis(retryConfig.getMaxRetryDelayMs().get()));
      }
      if (retryConfig.getInitialRpcTimeoutMs().isPresent()) {
        retrySettings.setInitialRpcTimeout(Duration.ofMillis(retryConfig.getInitialRpcTimeoutMs().get()));
      }
      if (retryConfig.getRpcTimeoutMultiplier().isPresent()) {
        retrySettings.setRpcTimeoutMultiplier(retryConfig.getRpcTimeoutMultiplier().get());
      }
      if (retryConfig.getMaxRpcTimeoutMs().isPresent()) {
        retrySettings.setMaxRpcTimeout(Duration.ofMillis(retryConfig.getMaxRpcTimeoutMs().get()));
      }
      if (retryConfig.getTotalTimeoutMs().isPresent()) {
        retrySettings.setTotalTimeout(Duration.ofMillis(retryConfig.getTotalTimeoutMs().get()));
      }
      bigQueryOptionsBuilder.setRetrySettings(retrySettings.build());
    }

    BigQuery bigQuery = bigQueryOptionsBuilder.build().getService();

    return new BigQueryDataSourceDao(
        new BigQueryExecutorImpl(jobStore, bigQueryJobConfig, bigQuery),
        gringottsClient, metricRegistry);
  }

  @Override
  public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
    return DataSourceType.BIG_QUERY.getDslConfig(dslConfig);
  }

  private BigQueryJobConfig getBigQueryJobConfig(Map<String, Object> attributes) {
    BigQueryJobConfig bigQueryJobConfig = BigQueryJobConfig.builder()
        .totalTimeoutLimitMs(Long.valueOf((String)
            attributes.get("totalTimeoutLimitMs")))
        .legacySql(Boolean.valueOf((String) attributes.getOrDefault("legacySql", "false")))
        .tableSizeLimitInMbs(Long.valueOf((String)
            attributes.get("tableSizeLimitInMbs")))
        .build();

    return bigQueryJobConfig;
  }

  private BigQueryClientConfig getBigQueryClientConfig(Map<String, Object> attributes) {
    Map<String,Object> retryConfigMap = (Map<String, Object>) attributes.get("clientRetryConfig");
    Optional<BigQueryClientConfig.RetryConfig> clientRetryConfig;
    if (retryConfigMap != null) {
      clientRetryConfig = Optional.fromNullable(JsonUtil.convertValue(retryConfigMap, BigQueryClientConfig.RetryConfig.class));
    } else {
      clientRetryConfig = Optional.absent();
    }

    BigQueryClientConfig bigQueryClientConfig = BigQueryClientConfig.builder()
            .projectId((String) attributes.get("projectId"))
            .clientRetryConfig(clientRetryConfig)
            .build();
    return bigQueryClientConfig;
  }
}

