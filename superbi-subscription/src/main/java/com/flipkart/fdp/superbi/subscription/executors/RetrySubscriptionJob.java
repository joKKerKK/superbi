package com.flipkart.fdp.superbi.subscription.executors;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.subscription.client.PlatoExecutionClient;
import com.flipkart.fdp.superbi.subscription.client.PlatoMetaClient;
import com.flipkart.fdp.superbi.subscription.client.SuperBiClient;
import com.flipkart.fdp.superbi.subscription.delivery.DeliveryExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.model.DeliveryData.DeliveryAction;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

@Slf4j
public class RetrySubscriptionJob extends AbstractSubscriptionJob {

  public RetrySubscriptionJob(
          SuperBiClient superBiClient,
          Map<DeliveryAction, DeliveryExecutor> deliveryAction, MetricRegistry metricRegistry,
          SubscriptionEventLogger subscriptionEventLogger, int maxSubscriptionRunsLeftForComm,
          int maxDaysLeftForComm, PlatoExecutionClient platoExecutionClient, PlatoMetaClient platoMetaClient) {
    super(superBiClient, platoExecutionClient, platoMetaClient, deliveryAction,metricRegistry, subscriptionEventLogger, maxSubscriptionRunsLeftForComm, maxDaysLeftForComm);
  }

  @Override
  protected void submitForRetry(Exception e, ScheduleInfo scheduleInfo,
      JobExecutionContext jobExecutionContext) throws JobExecutionException {

    log.info("Retrying job for scheduleId <{0}> with attempt <{1}>",scheduleInfo.getSubscriptionId(),
        scheduleInfo.getAttempt());

    JobExecutionException jobExecutionException = new JobExecutionException(e);
    jobExecutionContext.getMergedJobDataMap().putAll(JsonUtil.convertValue(scheduleInfo,Map.class));
    jobExecutionException.setRefireImmediately(true);
    throw jobExecutionException;
  }
}
