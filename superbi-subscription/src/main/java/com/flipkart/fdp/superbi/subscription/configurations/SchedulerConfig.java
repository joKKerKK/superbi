package com.flipkart.fdp.superbi.subscription.configurations;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Properties;
import lombok.Data;

@Data
public class SchedulerConfig {

  private String alertMail;
  private Boolean allowMultipleInstance;
  private Integer threadPoolSize = 1;
  private String url;
  private String user;
  private String password;
  private Integer maxConn;
  private String schedulerName;

  @JsonCreator
  public SchedulerConfig(@JsonProperty("alertMail") String alertMail, @JsonProperty("allowMultipleInstance") Boolean allowMultipleInstance,
      @JsonProperty("threadPoolSize")Integer threadPoolSize, @JsonProperty("url") String url, @JsonProperty("user")String user
      , @JsonProperty("password") String password, @JsonProperty("maxConn") Integer maxConn,
      @JsonProperty("schedulerName") String schedulerName) {
    this.alertMail = alertMail;
    this.allowMultipleInstance = allowMultipleInstance;
    this.threadPoolSize = threadPoolSize;
    this.url = url;
    this.user = user;
    this.password = password;
    this.maxConn = maxConn;
    this.schedulerName = schedulerName;
  }

  public Properties buildProps() {

    final Properties properties = new Properties();
    properties.put("org.quartz.jobStore.dataSource", "quartzDataSource");
    properties.put("org.quartz.scheduler.skipUpdateCheck", String.valueOf(true));
    properties.put("org.quartz.jobStore.clusterCheckinInterval","600000");
    properties.put("org.quartz.scheduler.wrapJobExecutionInUserTransaction",String.valueOf(false));
    properties.put("org.quartz.threadPool.threadCount", String.valueOf(getThreadPoolSize()));
    properties.put("org.quartz.scheduler.rmi.proxy",String.valueOf(false));
    properties.put("org.quartz.scheduler.instanceName",getSchedulerName());
    properties.put("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
    properties.put("org.quartz.jobStore.misfireThreshold","60000");
    properties.put("org.quartz.scheduler.batchTriggerAcquisitionFireAheadTimeWindow","150000");
    properties.put("org.quartz.jobStore.tablePrefix","QRTZ_");
    properties.put("org.quartz.dataSource.quartzDataSource.password", getPassword());
    properties.put("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread",String.valueOf(true));
    properties.put("org.quartz.dataSource.quartzDataSource.URL", getUrl());
    properties.put("org.quartz.jobStore.isClustered", String.valueOf(true));
    properties.put("org.quartz.scheduler.rmi.export",String.valueOf(false));
    properties.put("org.quartz.threadPool.threadPriority","5");
    properties.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
    properties.put("org.quartz.scheduler.instanceId", "AUTO");
    properties.put("org.quartz.dataSource.quartzDataSource.driver", "com.mysql.jdbc.Driver");
    properties.put("org.quartz.dataSource.quartzDataSource.maxConnections", String.valueOf(getMaxConn()));
    properties.put("org.quartz.dataSource.quartzDataSource.user", getUser());
    properties.put("org.quartz.jobStore.driverDelegateClass", "org.quartz.impl.jdbcjobstore.StdJDBCDelegate");
    properties.put("org.quartz.dataSource.quartzDataSource.validationQuery", "select 1");
    return properties;
  }
}
