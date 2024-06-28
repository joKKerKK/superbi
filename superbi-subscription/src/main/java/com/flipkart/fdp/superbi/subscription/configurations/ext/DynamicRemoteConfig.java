package com.flipkart.fdp.superbi.subscription.configurations.ext;

import com.flipkart.fdp.superbi.subscription.configurations.SubscriptionConfig;
import com.flipkart.fdp.superbi.subscription.configurations.RemoteSubscriptionConfig;
import com.flipkart.fdp.config.ConfigBuilder;
import com.flipkart.fdp.config.InstanceUpdateListner;
import com.flipkart.kloud.config.error.ConfigServiceException;
import java.io.IOException;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicRemoteConfig implements InstanceUpdateListner<RemoteSubscriptionConfig>,
    SubscriptionConfig {
  private static DynamicRemoteConfig dynamicRemoteConfig;

  private ConfigBuilder configBuilder;

  private DynamicRemoteConfig(ConfigBuilder configBuilder) {
    this.configBuilder = configBuilder;
  }

  public static SubscriptionConfig getInstance() throws IOException, ConfigServiceException {
    if (dynamicRemoteConfig == null) {
      ConfigBuilder configBuilder1 = new ConfigBuilder().getInstance();
      dynamicRemoteConfig = new DynamicRemoteConfig(configBuilder1);
      RemoteSubscriptionConfig config = configBuilder1.getAndAddListener(RemoteSubscriptionConfig.class, dynamicRemoteConfig);
      if(!config.build(configBuilder1)) {
        throw new RuntimeException("Invalid configs");
      }
      dynamicRemoteConfig.config = config;
    }

    return dynamicRemoteConfig;
  }

  @Delegate
  private RemoteSubscriptionConfig config;

  @Override
  public void onConfigUpdate(RemoteSubscriptionConfig updatedConfig) {
    if (updatedConfig.build(configBuilder)) {
      this.config = updatedConfig;
      log.info("New dynamic config applied");
    }else {
      log.error("Could not apply new dynamic config. Executing with old config {}", this.config);
    }
  }
}
