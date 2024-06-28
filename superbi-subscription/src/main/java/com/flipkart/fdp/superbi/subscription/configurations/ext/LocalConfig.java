package com.flipkart.fdp.superbi.subscription.configurations.ext;

import com.flipkart.fdp.superbi.subscription.configurations.SubscriptionConfig;
import com.flipkart.kloud.config.error.ConfigServiceException;
import java.io.IOException;
import java.util.Map;
import lombok.experimental.Delegate;

public class LocalConfig implements SubscriptionConfig {
  @Delegate(excludes = ExcludedListMethods.class)
  private final SubscriptionConfig config;

  public LocalConfig(SubscriptionConfig config) throws IOException, ConfigServiceException {
    this.config = config;
  }

  private abstract class ExcludedListMethods {
    public abstract Map<String, String> getPersistenceOverrides(String persistenceUnit);
  }
}
