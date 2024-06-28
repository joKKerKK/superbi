package com.flipkart.fdp.superbi.subscription.executors;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.subscription.client.PlatoExecutionClient;
import com.flipkart.fdp.superbi.subscription.client.PlatoMetaClient;
import com.flipkart.fdp.superbi.subscription.client.SuperBiClient;
import com.flipkart.fdp.superbi.subscription.delivery.DeliveryExecutor;
import com.flipkart.fdp.superbi.subscription.event.SubscriptionEventLogger;
import com.flipkart.fdp.superbi.subscription.model.DeliveryData.DeliveryAction;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import java.text.MessageFormat;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;

@Slf4j
public class SubscriptionJob extends AbstractSubscriptionJob{

  private final RetryJobHandler retryJobHandler;


  public SubscriptionJob(SuperBiClient superBiClient
      , Map<DeliveryAction,DeliveryExecutor> deliveryAction,
                         RetryJobHandler retryJobHandler, MetricRegistry metricRegistry,
                         SubscriptionEventLogger subscriptionEventLogger, int maxSubscriptionRunsLeftForComm,
                         int maxDaysLeftForComm, PlatoExecutionClient platoExecutionClient, PlatoMetaClient platoMetaClient){
    super(superBiClient, platoExecutionClient, platoMetaClient, deliveryAction,metricRegistry, subscriptionEventLogger, maxSubscriptionRunsLeftForComm, maxDaysLeftForComm);
    this.retryJobHandler = retryJobHandler;
  }

  @Override
  protected void submitForRetry(Exception e,ScheduleInfo scheduleInfo,JobExecutionContext jobExecutionContext) {
    retryJobHandler.submitForRetry(scheduleInfo);
    log.error(MessageFormat.format("Got exception {0} for scheduleId : <{1}>",e.getMessage(),jobExecutionContext.getTrigger().getKey()));

  }

}
