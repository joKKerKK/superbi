package com.flipkart.fdp.superbi.brv2.config.ext;

import com.flipkart.fdp.config.ConfigBuilder;
import com.flipkart.fdp.config.InstanceUpdateListner;
import com.flipkart.fdp.superbi.brv2.config.ApplicationConfig;
import com.flipkart.fdp.superbi.brv2.config.RemoteApplicationConfig;
import com.flipkart.kloud.authn.AuthTokenService;
import com.flipkart.kloud.config.ConfigClient;
import com.flipkart.kloud.config.ConfigClientBuilder;
import com.flipkart.kloud.config.error.ConfigServiceException;
import com.flipkart.security.cryptex.CryptexClient;
import com.flipkart.security.cryptex.CryptexClientBuilder;
import java.io.IOException;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by akshaya.sharma on 30/07/19
 */
@Slf4j
public class DynamicRemoteConfig implements InstanceUpdateListner<RemoteApplicationConfig>,
    ApplicationConfig {
  public static final String CRYPTEX_CLIENT_ID = "CRYPTEX_CLIENT_ID";
  public static final String CRYPTEX_CLIENT_SECRET = "CRYPTEX_CLIENT_SECRET";
  public static final String CRYPTEX_AUTHN_URL = "CRYPTEX_AUTHN_URL";

  private static DynamicRemoteConfig dynamicRemoteConfig;

  private ConfigBuilder configBuilder;

  private DynamicRemoteConfig(ConfigBuilder configBuilder) {
    this.configBuilder = configBuilder;
  }

  public static ApplicationConfig getInstance() throws IOException, ConfigServiceException {
    if (dynamicRemoteConfig == null) {
      CryptexClient cryptexClient = new CryptexClientBuilder(getAuthTokenService()).build();
      ConfigClient client = new ConfigClientBuilder().cryptexClient(cryptexClient).build();
      ConfigBuilder configBuilder1 = new ConfigBuilder(client);
      dynamicRemoteConfig = new DynamicRemoteConfig(configBuilder1);
      RemoteApplicationConfig config = configBuilder1.getAndAddListener(RemoteApplicationConfig.class, dynamicRemoteConfig);

      if(!config.build(configBuilder1)) {
        throw new RuntimeException("Invalid configs");
      }
      dynamicRemoteConfig.config = config;
    }
    return dynamicRemoteConfig;
  }

  private final static AuthTokenService getAuthTokenService(){
    final String CLIENT_ID = System.getProperty(CRYPTEX_CLIENT_ID);
    final String CLIENT_SECRET = System.getProperty(CRYPTEX_CLIENT_SECRET);
    final String AUTHN_URL = System.getProperty(CRYPTEX_AUTHN_URL); //authn endpoints
    AuthTokenService.init(AUTHN_URL, CLIENT_ID, CLIENT_SECRET);
    return AuthTokenService.getInstance();
  }

  @Delegate
  private RemoteApplicationConfig config;

  @Override
  public void onConfigUpdate(RemoteApplicationConfig updatedConfig) {
    if (updatedConfig.build(configBuilder)) {
      this.config = updatedConfig;
      log.info("New dynamic config applied");
    }else {
      log.error("Could not apply new dynamic config. Executing with old config.");
    }
  }
}
