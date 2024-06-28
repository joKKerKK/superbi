package com.flipkart.fdp.superbi.subscription.executors;

import static org.quartz.TriggerBuilder.newTrigger;

import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.quartz.Scheduler;
import org.quartz.SimpleTrigger;

@Slf4j
@AllArgsConstructor
public class DefaultRetryJobHandler implements RetryJobHandler {

  private static final String JOB_NAME = "subscription";
  public static final String RETRY_GROUP = "retry_group";
  private final Scheduler scheduler;
  private final long backOffTimeInMillis;

  @Override
  public void submitForRetry(ScheduleInfo scheduleInfo) {
    try {
      log.info(MessageFormat.format("submitting task for retry for scheduleId <{0}>",scheduleInfo.getSubscriptionId()));
      long startTime = new Date().getTime() + backOffTimeInMillis;

      final SimpleTrigger trigger = (SimpleTrigger) newTrigger()
          .withIdentity(getTriggerName(scheduleInfo), RETRY_GROUP)
          .startAt(new Date(startTime))
          .forJob(JOB_NAME)
          .build();


      trigger.getJobDataMap().putAll(JsonUtil.convertValue(scheduleInfo,Map.class));

      scheduler.scheduleJob(trigger);
      log.info(MessageFormat.format("Retry trigger creation successful for scheduleId <{0}>",scheduleInfo.getSubscriptionId()));
    }catch (Exception e){
      log.error(MessageFormat.format("Failed to create retry schedule <{0}> due to <{1}>",scheduleInfo.getSubscriptionId(),e.getMessage()));
    }
  }

  private String getTriggerName(ScheduleInfo scheduleInfo) {
    return StringUtils.join(Arrays.asList(String.valueOf(scheduleInfo.getSubscriptionId()),"retry",scheduleInfo.getAttempt()),'_');
  }
}
