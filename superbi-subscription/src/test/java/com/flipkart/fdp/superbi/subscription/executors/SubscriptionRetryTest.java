package com.flipkart.fdp.superbi.subscription.executors;

import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo.DeliveryType;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo.ResourceType;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.util.Date;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class SubscriptionRetryTest {

  AbstractSubscriptionJob subscriptionJob = new SubscriptionJob(null, null, null, null, null, 7, 30, null, null);

  @Test
  public void testWithNextFireTimeNull() {
    Long nextFireTime = null;
    long triggerTime = new Date().getTime() - 100;
    ScheduleInfo scheduleInfo = new ScheduleInfo("org", "namespace", "name", "123",
        ResourceType.REPORT, DeliveryType.CSV,
        new HashMap<>(), "subs", 1, null, new HashMap<>() ,triggerTime, "123", 0, nextFireTime,
        "cron", "1");
    Assert.assertTrue(subscriptionJob.checkIfRetryable(scheduleInfo, new RuntimeException("e")));
  }

  @Test
  public void testWithNextFireTimeLessThanCurrentTime() {
    long nextFireTime = new Date().getTime() - 100000;
    long triggerTime = new Date().getTime() - 100;
    ScheduleInfo scheduleInfo = new ScheduleInfo("org", "namespace", "name", "123",
        ResourceType.REPORT, DeliveryType.CSV,
        new HashMap<>(), "subs", 1, null, new HashMap<>() ,triggerTime, "123", 0, nextFireTime,
        "cron", "1");
    Assert.assertFalse(subscriptionJob.checkIfRetryable(scheduleInfo, new RuntimeException("e")));
  }


  @Test
  public void testWithNextFireTimeGreaterThanCurrentTime() {
    long nextFireTime = new Date().getTime() + 100000;
    long triggerTime = new Date().getTime() - 100;
    ScheduleInfo scheduleInfo = new ScheduleInfo("org", "namespace", "name", "123",
        ResourceType.REPORT, DeliveryType.CSV,
        new HashMap<>(), "subs", 1, null, new HashMap<>(),triggerTime, "123", 0, nextFireTime, "cron", "1");
    Assert.assertTrue(subscriptionJob.checkIfRetryable(scheduleInfo, new RuntimeException("e")));
  }

  @Test
  public void testWitTriggerTimeMoreThanADay() {
    long nextFireTime = new Date().getTime() + 100000;
    long triggerTime = new Date().getTime() - 86400001;
    ScheduleInfo scheduleInfo = new ScheduleInfo("org", "namespace", "name", "123",
        ResourceType.REPORT, DeliveryType.CSV,
        new HashMap<>(), "subs", 1, null, new HashMap<>(), triggerTime, "123", 0, nextFireTime,
        "cron", "1");
    Assert.assertFalse(subscriptionJob.checkIfRetryable(scheduleInfo, new RuntimeException("e")));
  }

  @Test
  public void testWitClientSideException() {
    long nextFireTime = new Date().getTime() + 100000;
    long triggerTime = new Date().getTime() - 100;
    ScheduleInfo scheduleInfo = new ScheduleInfo("org", "namespace", "name", "123",
        ResourceType.REPORT, DeliveryType.CSV,
        new HashMap<>(), "subs", 1, null, new HashMap<>(), triggerTime, "123", 0, nextFireTime,
        "cron", "1");
    Assert
        .assertFalse(subscriptionJob.checkIfRetryable(scheduleInfo, new ClientSideException("e")));
  }

  @Test
  public void testWitCallNotpermittedException() {
    long nextFireTime = new Date().getTime() + 100000;
    long triggerTime = new Date().getTime() - 100;
    ScheduleInfo scheduleInfo = new ScheduleInfo("org", "namespace", "name", "123",
        ResourceType.REPORT, DeliveryType.CSV,
        new HashMap<>(), "subs", 1, null, new HashMap<>(), triggerTime, "123", 0, nextFireTime,
        "cron", "1");
    Assert.assertTrue(subscriptionJob
        .checkIfRetryable(scheduleInfo, CallNotPermittedException.createCallNotPermittedException(
            CircuitBreaker.ofDefaults("n"))));
  }

}
