package com.flipkart.fdp.superbi.refresher.dao.bigquery.executor;

import com.flipkart.fdp.superbi.refresher.dao.bigquery.BigQueryJobData;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import java.util.Map;
import java.util.Optional;

/**
 * Created by mansi.jain on 21/02/22
 */
public interface BigQueryExecutor {

  void removeJobData(String jobKey);

  Optional<BigQueryJobData> getJobData(String jobKey);

  Job createQueryJob(String query, String jobKey, Map<String,String> billingLabels);

  Job getJob(JobId jobId);

  Boolean isResultProcessable(Job queryJob);

  long getDataSizeLimit();
}
