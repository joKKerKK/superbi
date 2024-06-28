package com.flipkart.fdp.superbi.cosmos.queryTranslator;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.mmg.cosmos.entities.DataSource;
import com.flipkart.fdp.superbi.cosmos.BQUsageType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQBatchQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQBatchV2QueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.bq.BQRealtimeQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidQuery;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.EuclidQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticQueryType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.FStreamParserConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.FStreamQuery;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.FStreamQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.FstreamRequest;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql.MysqlDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql.MysqlQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.hive.HiveDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.hive.HiveQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class CosmosNativeQueryTranslator {

  private static final String NATIVE_TRANSLATION__METRIC_KEY = "transaltion.time";
  public static final int MAX_STRING_SIZE = 60000;

  private final Map<String, AbstractDSLConfig> dslConfigMap;
  private final MetricRegistry metricRegistry;
  private final BadgerClient badgerClient;

  public CosmosNativeQueryTranslator(Map<String, AbstractDSLConfig> dslConfigMap,
      MetricRegistry metricRegistry, BadgerClient badgerClient) {
    this.dslConfigMap = dslConfigMap;
    this.metricRegistry = metricRegistry;
    this.badgerClient = badgerClient;
  }

  public CosmosNativeQueryTranslator(Map<String, AbstractDSLConfig> dslConfigMap,
                                     MetricRegistry metricRegistry) {
    this(dslConfigMap, metricRegistry, BadgerClient.getInstance());
  }

  @SneakyThrows
  public NativeQuery getNativeQuerySource(String storeIdentifier, DSQuery query,
      Map<String, String[]> params,
      Map<String,String> federationProperties, Function<String, Map<String, String>> getTableProps, Function<String, DataSource> getDataSource,
      List<String> euclidRules) {
    try (Timer.Context context = getNativeQueryTranslationTimer(storeIdentifier)) {
      AbstractQueryBuilder queryBuilder = getQueryBuilder(query, params, storeIdentifier, federationProperties, getTableProps, getDataSource, euclidRules);
      Object nativeQuery = queryBuilder.buildQuery();
      // TODO instrumentations

      if (nativeQuery instanceof FStreamQuery) {
        FStreamQuery fStreamQuery = (FStreamQuery) nativeQuery;
        nativeQuery = transformCosmosQuery(fStreamQuery);
      } else if(nativeQuery instanceof ESQuery) {
        nativeQuery = new com.flipkart.fdp.superbi.refresher.dao.elastic.requests.ESQuery((ESQuery) nativeQuery);
      } else if(nativeQuery instanceof DruidQuery){
        DruidQuery druidQuery = (DruidQuery) nativeQuery;
        nativeQuery = new com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidQuery(
            druidQuery.getQuery(),druidQuery.getHeaderList(),druidQuery.getContext());
      }

      // `nativeQuery` is a jackson serializable and deserializable object
      return new NativeQuery(nativeQuery);

    }catch (Exception e) {
      log.warn(MessageFormat
          .format("Native query generation for DSQuery failed {0}", e.getMessage()));
      throw e;
    }
  }

  private Timer.Context getNativeQueryTranslationTimer(String storeIdentifier) {
    return metricRegistry.timer(getMetricsKey(NATIVE_TRANSLATION__METRIC_KEY, storeIdentifier))
        .time();
  }

  private String getMetricsKey(String prefix, String storeIdentifier) {
    return StringUtils.join(Arrays.asList(prefix, storeIdentifier), '.');
  }

  private com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FStreamQuery transformCosmosQuery(
      FStreamQuery fStreamQuery) {
    com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest transformedRequest = getTransformedRequest(
        fStreamQuery.getFstreamRequest());
    return new com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FStreamQuery(
        transformedRequest, fStreamQuery.getFstreamId(), fStreamQuery.getOrderedFstreamColumns());
  }

  @SneakyThrows
  private com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest getTransformedRequest(
      FstreamRequest fstreamRequest) {
    return JsonUtil.fromJson(JsonUtil.toJson(fstreamRequest),
        com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest.class);
  }

  private AbstractQueryBuilder getQueryBuilder(DSQuery query,
      Map<String, String[]> paramValues, String storeIdentifier,
      Map<String, String> federationProperties,
      Function<String, Map<String, String>> getTableProps,
      Function<String, DataSource> getDataSource,
      List<String> euclidRules) {
    AbstractDSLConfig dslConfig = dslConfigMap.get(storeIdentifier);
    if (dslConfig instanceof ElasticParserConfig) {
      return ElasticQueryType.getFor(query)
          .getBuilder(query, paramValues, (ElasticParserConfig) dslConfig);
    } else if(dslConfig instanceof BQDSLConfig) {
      BQUsageType usageType = ((BQDSLConfig) dslConfig).getUsageType();
      if (usageType == BQUsageType.BQ_BATCH) {
        return new BQBatchQueryBuilder(query, paramValues, dslConfig,
            federationProperties,getTableProps, getDataSource);
      }
      if (usageType == BQUsageType.BQ_REALTIME) {
        return new BQRealtimeQueryBuilder(query, paramValues, dslConfig,
            federationProperties, getDataSource);
      }
      if (usageType == BQUsageType.BQ_BATCH_V2) {
          return new BQBatchV2QueryBuilder(query, paramValues, dslConfig,
              federationProperties,getTableProps,getDataSource);
      }
    } else if (dslConfig instanceof VerticaDSLConfig) {
      return new VerticaQueryBuilder(query, paramValues, (VerticaDSLConfig) dslConfig);
    } else if (dslConfig instanceof HiveDSLConfig) {
      return new HiveQueryBuilder(query, paramValues, (HiveDSLConfig) dslConfig, false, badgerClient);
    } else if (dslConfig instanceof FStreamParserConfig) {
      return new FStreamQueryBuilder(query, paramValues, (FStreamParserConfig) dslConfig);
    } else if (dslConfig instanceof MysqlDSLConfig) {
      return new MysqlQueryBuilder(query, paramValues, (MysqlDSLConfig) dslConfig);
    } else if(dslConfig instanceof DruidDSLConfig){
      Boolean enableValidations = ((DruidDSLConfig) dslConfig).getEnableValidations();
      if (enableValidations){
        return new EuclidQueryBuilder(query, paramValues, (DruidDSLConfig) dslConfig, euclidRules);
      } else {
        return new DruidQueryBuilder(query, paramValues, (DruidDSLConfig) dslConfig);
      }
    }
    throw new RuntimeException(MessageFormat
        .format("Config not defined for data source with store Identifier {0}", storeIdentifier));
  }
}