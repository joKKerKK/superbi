package com.flipkart.fdp.superbi.refresher.dao.bigquery.executor;

import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.BigQueryJobConfig;
import com.flipkart.fdp.superbi.refresher.dao.bigquery.BigQueryJobData;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mansi.jain on 21/02/22
 */
@AllArgsConstructor
@Slf4j
public class BigQueryExecutorImpl implements BigQueryExecutor {

  private final CacheDao jobStore;
  private final BigQueryJobConfig bigQueryJobConfig;
  private final BigQuery bigQuery;

  @Override
  public void removeJobData(String jobKey) {
    jobStore.remove(jobKey);
  }

  @Override
  public Optional<BigQueryJobData> getJobData(String jobKey) {
    return jobStore.get(jobKey, BigQueryJobData.class);
  }

  @Override
  public Job getJob(JobId jobId) {
    return bigQuery.getJob(jobId);
  }

  @Override
  public Boolean isResultProcessable(Job queryJob) {
    TableId tableId = ((QueryJobConfiguration) queryJob.getConfiguration()).getDestinationTable();
    Table table = bigQuery.getTable(tableId);
    // get the size of temporary table to guardrail result size
    Long tableNumBytes = table.getNumBytes();

    if(bigQueryJobConfig.getTableSizeLimitInMbs() == null)
      return true;

    if (tableNumBytes > bigQueryJobConfig.getTableSizeLimitInMbs()*1024*1204) {
      return false;
    }
    return true;
  }

  @Override
  public Job createQueryJob(String query, String jobKey, Map<String,String> billingLabels) {
    log.info("Starting new execution");
    QueryJobConfiguration.Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(bigQueryJobConfig.isLegacySql())
        .setJobTimeoutMs(bigQueryJobConfig.getTotalTimeoutLimitMs());
    if(billingLabels != null) {
      queryConfigBuilder.setLabels(billingLabels);
    }
    QueryJobConfiguration queryConfig = queryConfigBuilder.build();
    JobId jobId = JobId.of(getNewJobId(jobKey));
    Job queryJob = null;
    try{
      queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
      setJobData(jobKey, jobId.getJob());
    } catch (Throwable e) {
      log.error(
          "Cannot create job at bigquery with job key {} with message {}",
          jobKey, e.getMessage());
      throw e;
    }
    return queryJob;
  }

  private String getNewJobId(String cacheKey) {
    return String.join("_", cacheKey, String.valueOf(System.currentTimeMillis()));
  }

  private void setJobData(String jobKey, String jobId) {
    // setting ttl for object in cache to totalTimeoutLimitMs+5s
    int ttl = (int) (bigQueryJobConfig.getTotalTimeoutLimitMs() / 1000) + 5;
    jobStore.set(jobKey, ttl, new BigQueryJobData(jobId));
  }

  @Override
  public long getDataSizeLimit(){
    return bigQueryJobConfig.getTableSizeLimitInMbs();
  }
}
