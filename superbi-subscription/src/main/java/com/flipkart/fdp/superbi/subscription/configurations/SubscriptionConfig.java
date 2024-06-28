package com.flipkart.fdp.superbi.subscription.configurations;

import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.gcs.GcsConfig;
import com.google.common.collect.Maps;
import emailsvc.Connekt.ConnektServiceConfig;
import java.util.Map;

public interface SubscriptionConfig {

  SuperbiClientConfig getSuperbiClientConfig();

  PlatoMetaClientConfig getPlatoMetaClientConfig();

  PlatoExecutionClientConfig getPlatoExecutionClientConfig();

  long getD42ExpiryInSeconds();

  D42Configuration getD42Configuration();

  GcsConfig getGcsConfig();

  SchedulerConfig getSchedulerConfig();

  ConnektServiceConfig getConnektServiceConfig();

  boolean isAuditorEnabled();
  int getDBAuditorRequestTimeout();
  DefaultEmailConfig getDefaultEmailClientConfig();

  GsheetConfig getGsheetConfig();

  String getEmailTemplate();

  String getGsheetCreationEmailTemplate();

  String getGsheetCancelledEmailTemplate();

  String getGsheetOverwriteEmailTemplate();

  String getFailureEmailTemplate();

  String getExpirationCommTemplate();

  SubscriptionJobConfig getSubscriptionJobConfig();

  int getMaxSubscriptionRunsLeftForComm();

  int getMaxDaysLeftForComm();

  default Map<String, String> getPersistenceOverrides(String persistenceUnit) {
    return Maps.newHashMap();
  }

}
