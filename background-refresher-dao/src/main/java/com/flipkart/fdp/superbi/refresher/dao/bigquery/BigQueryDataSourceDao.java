package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.exceptions.RateLimitServerSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.exceptions.SuperBiException;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.executor.BigQueryExecutor;
import com.flipkart.fdp.superbi.refresher.dao.exceptions.DataSizeLimitExceedException;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.threeten.bp.Duration;

import static com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient.HYPHEN_DELIMITER;
import static com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient.MAX_LABEL_SIZE;


@Slf4j
public class BigQueryDataSourceDao implements DataSourceDao {

  private final BigQueryExecutor bigQueryExecutor;
  private final GringottsClient gringottsClient;
  private final Meter rateLimitMeter;
  private final MetricRegistry metricRegistry;
  private final BigQueryExceptionMap bigQueryExceptionMap = new BigQueryExceptionMap();
  private static Map<LegacySQLTypeName, Function<FieldValue, Object>> typeToValueConverter = BigQueryConverterMap.getTypeToValueConverter();

  Function<FieldValue, Object> getDefaultValue = (fieldValue) ->
      Objects.isNull(fieldValue.getValue()) ? null
          : fieldValue.getStringValue();

  public BigQueryDataSourceDao(BigQueryExecutor bigQueryExecutor, GringottsClient gringottsClient,
      MetricRegistry metricRegistry) {
    this.bigQueryExecutor = bigQueryExecutor;
    this.gringottsClient = gringottsClient;
    this.metricRegistry = metricRegistry;
    this.rateLimitMeter = metricRegistry.meter("bigquery.rateLimitBreached");
  }

  @Override
  public QueryResult getStream(DataSourceQuery nativeQuery) {
    try {
      Optional<BigQueryJobData> jobIdOptional = bigQueryExecutor.getJobData(
          nativeQuery.getCacheKey());
      Job queryJob = getOrCreateQueryJob(nativeQuery, jobIdOptional);
      return pollAndGetResult(queryJob);
    } catch (SuperBiException ex) {
      throw ex;
    } catch (BigQueryException bigQueryException) {
      String causeType =
          bigQueryException.getError() != null ? bigQueryException.getError().getReason()
              : bigQueryException.getReason();
      String message =
          bigQueryException.getError() != null ? bigQueryException.getError().getMessage()
              : bigQueryException.getMessage();
      String errorMessage = String.format(
          "Query with cache key: %s, failed at BigQuery with status %s : %s : %s",
          nativeQuery.getCacheKey(), bigQueryException.getCode(), causeType, message);

      if ("rateLimitExceeded".equals(causeType)) {
        rateLimitMeter.mark();
        throw new RateLimitServerSideException(errorMessage, bigQueryException);
      }

      throw bigQueryExceptionMap.buildException(causeType, errorMessage, bigQueryException);
    } catch (Exception e) {
      log.error("Query Execution failed for query {} due to {}", nativeQuery.getMetaDataPayload().toString(),
          e.getMessage());
      throw new ServerSideException(e);
    } finally {
      bigQueryExecutor.removeJobData(nativeQuery.getCacheKey());
    }
  }

  @SneakyThrows
  private Job getOrCreateQueryJob(DataSourceQuery nativeQuery,
      Optional<BigQueryJobData> jobIdOptional) {
    Map<String, String> bqLabels = generateBQLabels(nativeQuery);

    Job queryJob;
    if (jobIdOptional.isPresent()) {
      JobId jobId = JobId.of(jobIdOptional.get().getJobId());
      queryJob = bigQueryExecutor.getJob(jobId);
      if (queryJob != null && queryJob.getStatus().getState() != JobStatus.State.DONE) {
        return queryJob;
      }
    }
    String query = String.valueOf(nativeQuery.getNativeQuery());
    return bigQueryExecutor.createQueryJob(query, nativeQuery.getCacheKey(), bqLabels);
  }

  @SneakyThrows
  private QueryResult pollAndGetResult(Job queryJobInput) {
    log.info("Polling and getting result in BQ for jobId {}", queryJobInput.getJobId());
    Job queryJobResult = queryJobInput.waitFor(
        RetryOption.initialRetryDelay(Duration.ofMillis(2000)));
    if (queryJobResult == null) {
      log.warn("Job id {} no longer exists.", queryJobInput.getJobId().getJob());
      throw new ServerSideException(
          String.format("Job %s no longer exists.", queryJobInput.getJobId()));
    }
    if (queryJobResult != null && queryJobResult.getStatus().getError() != null) {
      throw new BigQueryException(
          CollectionUtils.isEmpty(queryJobResult.getStatus().getExecutionErrors()) ? ImmutableList.of(
              queryJobResult.getStatus().getError())
              : ImmutableList.copyOf(queryJobResult.getStatus().getExecutionErrors()));
    }

    return buildQueryResult(queryJobResult);
  }

  private Map<String,String> generateBQLabels(DataSourceQuery nativeQuery) {
    Map<String,String> bqLabels = gringottsClient.getBillingLabels(nativeQuery.getMetaDataPayload().getUsername());
    if (bqLabels == null) {
      bqLabels = gringottsClient.getNewBillingLabels(nativeQuery.getMetaDataPayload().getUsername());
    }
    if (bqLabels == null) {
      bqLabels = new HashMap<>();
    }
    log.info("nativeQuery {}", nativeQuery.getMetaDataPayload().toString());
    if (nativeQuery.getCacheKey() != null) {
      bqLabels.put("cachekey", refactorLabelValue(nativeQuery.getCacheKey()));
    } else {
      bqLabels.put("cachekey", "na");
    }
    if (nativeQuery.getMetaDataPayload().getClient() != null) {
      bqLabels.put("client", refactorLabelValue(nativeQuery.getMetaDataPayload().getClient()));
    } else {
      bqLabels.put("client", "na");
    }
    if (nativeQuery.getMetaDataPayload().getReportName() != null) {
      bqLabels.put("reportname", refactorLabelValue(nativeQuery.getMetaDataPayload().getReportName()));
    } else {
      bqLabels.put("reportname", "na");
    }
    if (nativeQuery.getMetaDataPayload().getFactName() != null) {
      bqLabels.put("factname", refactorLabelValue(nativeQuery.getMetaDataPayload().getFactName()));
    } else {
      bqLabels.put("factname", "na");
    }
    if (nativeQuery.getMetaDataPayload().getUsername() != null) {
      bqLabels.put("user", refactorLabelValue(nativeQuery.getMetaDataPayload().getUsername()));
    } else {
      bqLabels.put("user", "na");
    }
    if (nativeQuery.getMetaDataPayload().getExecutionEngineLabels() != null) {
      Map<String, String> executionEngineLabels = nativeQuery.getMetaDataPayload().getExecutionEngineLabels();
      Map<String, String> transformedExecutionEngineLabels = executionEngineLabels.entrySet().stream()
              .collect(Collectors.toMap(
                      entry -> refactorLabelValue(entry.getKey()),
                      entry -> refactorLabelValue(entry.getValue())
              ));
      bqLabels.putAll(transformedExecutionEngineLabels);
    }
    return bqLabels;
  }
  public static String refactorLabelValue(String labelValue) {
    labelValue = labelValue.toLowerCase().replaceAll("[^a-z0-9-_]", HYPHEN_DELIMITER);
    return labelValue.length() > MAX_LABEL_SIZE ? labelValue.substring(0, MAX_LABEL_SIZE) : labelValue;
  }

  @SneakyThrows
  private QueryResult buildQueryResult(Job queryJob) {
    Meter iterationErrorMeter = metricRegistry.meter("bigquery.iterationError");
    if (bigQueryExecutor.isResultProcessable(queryJob)) {
      TableResult result = queryJob.getQueryResults();
      List<String> columnNames = result.getSchema().getFields().stream().map(Field::getName)
          .collect(Collectors.toList());
      List<LegacySQLTypeName> legacySQLTypeNames = result.getSchema().getFields().stream()
          .map(Field::getType).collect(Collectors.toList());
      Iterator<FieldValueList> bigQueryIterator = result.iterateAll().iterator();
      log.info("Result fetched for jobId {}", queryJob.getJobId().getJob());
      return new QueryResult() {
        @Override
        public Iterator<List<Object>> iterator() {
          return new Iterator<List<Object>>() {
            @Override
            public boolean hasNext() {
              try(Timer.Context timer = metricRegistry.timer("bigquery.fetch.time").time()) {
                // BQ iterator internally do a listTableData API call to lazy load results
                // which can fail
                return bigQueryIterator.hasNext();
              } catch (Exception exception) {
                iterationErrorMeter.mark();
                String message = String.format(
                    "Exception while fetching results for BQ Job %s. Root cause: %s",
                    queryJob.getJobId(), exception.getMessage());
                log.error(message, exception);
                throw new ServerSideException(message, exception);
              }
            }

            @Override
            public List<Object> next() {
              List<FieldValue> fieldValues = bigQueryIterator.next().stream()
                  .collect(Collectors.toList());
              List<Object> result = new ArrayList<>();
              for (int i = 0; i < columnNames.size(); i++) {
                String attribute = fieldValues.get(i).getAttribute().toString();
                if (attribute.equals("REPEATED")) {
                  int number = i;
                  List<Object> objects = fieldValues.get(i).getRepeatedValue().stream()
                      .map(fieldValue -> typeToValueConverter.getOrDefault(legacySQLTypeNames.get(
                              number), getDefaultValue)
                          .apply(fieldValue)).collect(Collectors.toList());
                  String values = objects.stream()
                      .map(value -> String.valueOf(value))
                      .collect(Collectors.joining(","));
                  result.add(values);
                } else {
                  result.add(i,
                      typeToValueConverter.getOrDefault(legacySQLTypeNames.get(i), getDefaultValue)
                          .apply(fieldValues
                              .get(i)));
                }
              }
              return result;
            }
          };
        }

        @Override
        public List<String> getColumns() {
          return columnNames;
        }

        @Override
        public void close() {
          //No implementation required
        }
      };
    } else {
      throw new DataSizeLimitExceedException(
          String.format("Data Size limit %s GBs exceed",
              bigQueryExecutor.getDataSizeLimit()/1024));
    }
  }

}
