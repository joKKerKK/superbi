package com.flipkart.fdp.superbi.subscription.configurations.quartz;

import com.google.inject.Inject;
import com.google.inject.Injector;
import lombok.SneakyThrows;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

public class QuartzJobFactory implements JobFactory {
  private final Injector injector;

  @Inject
  public QuartzJobFactory(Injector injector) {
    this.injector = injector;
  }

  @Override
  @SneakyThrows
  public Job newJob(TriggerFiredBundle triggerFiredBundle, Scheduler scheduler) {
    final JobDetail jobDetail = triggerFiredBundle.getJobDetail();
    try {
      return injector.getInstance((Class<? extends Job>)jobDetail.getJobClass());
    }
    catch (Exception e) {
      throw new SchedulerException(e);
    }
  }
}