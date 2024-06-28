package com.flipkart.fdp.superbi.brv2.execution;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.compito.api.clients.consumer.ConsumerRecord;
import com.flipkart.fdp.compito.api.command.Command;
import com.flipkart.fdp.superbi.brv2.SuperBiMessage;
import com.flipkart.fdp.superbi.execution.BackgroundRefreshTask;
import com.flipkart.fdp.superbi.execution.BackgroundRefreshTaskExecutor;
import com.flipkart.fdp.superbi.execution.RetryTaskHandler;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import lombok.AllArgsConstructor;
import reactor.core.publisher.Mono;

@AllArgsConstructor
public class SuperBiCommand implements
    Command<String, SuperBiMessage, QueryResult> {

  private final ConsumerRecord<String, SuperBiMessage> consumerRecord;
  private final BackgroundRefreshTaskExecutor taskExecutor;
  private final BackgroundRefresherConfig backgroundRefresherConfig;
  private final LockDao lockDao;
  private final RetryTaskHandler retryTaskHandler;
  private final MetricRegistry metricRegistry;

  @Override
  public Mono<QueryResult> execute() {
    return taskExecutor.executeTaskAsync(createTask());
  }

  @Override
  public ConsumerRecord<String, SuperBiMessage> getRequest() {
    return consumerRecord;
  }


  private BackgroundRefreshTask createTask() {
    return BackgroundRefreshTask.builder().backgroundRefresherConfig(backgroundRefresherConfig)
        .backgroundRefreshTaskExecutor(taskExecutor)
        .executionAfterTimestamp(consumerRecord.value().getExecuteAfter())
        .lockDao(lockDao)
        .queryPayload(consumerRecord.value().getQueryPayload())
        .remainingRetry(consumerRecord.value().getRemainingRetries())
        .retryTaskHandler(retryTaskHandler).build();
  }
}
