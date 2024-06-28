package com.flipkart.fdp.superbi.web.configurations.ext;

import com.flipkart.fdp.superbi.web.configurations.ApplicationConfig;
import com.flipkart.fdp.superbi.web.configurations.RemoteApplicationConfig;
import com.flipkart.fdp.utils.cfg.v2.DefaultConfigService;
import com.flipkart.fdp.utils.cfg.v2.dynamic.ConfigChangeListener;
import com.flipkart.fdp.utils.cfg.v2.dynamic.DynamicConfigServiceImpl;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 30/07/19
 */
@Slf4j
public class DynamicRemoteConfig implements ConfigChangeListener,ApplicationConfig {
  private static DynamicRemoteConfig dynamicRemoteConfig;

  private static DynamicConfigServiceImpl configService;

  private static final String APP_CONFIGMAP = "APP_CONFIGMAP";
  private static final String POD_NAMESPACE = "POD_NAMESPACE";
  private static final String APP_CONFIGMAP_SUB_PATH = "APP_CONFIGMAP_SUB_PATH";

  private static final String CFG_SVC_BUCKET = "CFG_SVC_BUCKET";

  private DynamicRemoteConfig(){
  }

  // This constructor is only for testing purposes
  DynamicRemoteConfig(RemoteApplicationConfig config) {
    this.config = config;
  }

  public static ApplicationConfig getInstance() {
    if (dynamicRemoteConfig == null) {
      dynamicRemoteConfig = new DynamicRemoteConfig();
      configService = new DynamicConfigServiceImpl(dynamicRemoteConfig);
      initializeConfigService();
      RemoteApplicationConfig config = new RemoteApplicationConfig(configService);
      if(!config.build()) {
        throw new RuntimeException("Invalid configs");
      }
      dynamicRemoteConfig.config = config;
    }

    return dynamicRemoteConfig;
  }

  @Delegate
  private RemoteApplicationConfig config;

  private static void initializeConfigService() {
    configService.setValueSubstitutor(DefaultConfigService.SYSTEM_ENV_SUBSTITUTOR);
    String configMapName = System.getenv(APP_CONFIGMAP);
    String namespace = System.getenv(POD_NAMESPACE);
    if(StringUtils.isNotEmpty(configMapName) && StringUtils.isNotEmpty(namespace)) {
      log.info("Using Config Map");
      String subPath = System.getenv(APP_CONFIGMAP_SUB_PATH);
      configService.initialize(configMapName, namespace, subPath);
    }else {
      log.info("Using Config Bucket");
      configService.initialize(getBucketName());
    }
  }

  private static String getBucketName() {
    String cfgSvcBucket = System.getProperty(CFG_SVC_BUCKET);
    if (null == cfgSvcBucket) {
      cfgSvcBucket = System.getenv(CFG_SVC_BUCKET);
    }
    if (!StringUtils.isEmpty(cfgSvcBucket)) {
      return cfgSvcBucket;
    } else {
      throw new RuntimeException("Config bucket undefined. Set property CFG_SVC_BUCKET to bucket name.");
    }
  }

  @Override
  public boolean onUpdate(DefaultConfigService defaultConfigService) {
    defaultConfigService.setValueSubstitutor(DefaultConfigService.SYSTEM_ENV_SUBSTITUTOR);
    config.setConfigService(defaultConfigService);
    if (!config.build()) {
      log.error("Failed to build configs after dynamic change");
      return false;
    }
    log.info("Dynamic configs patched successfully");
    return true;
  }
}
