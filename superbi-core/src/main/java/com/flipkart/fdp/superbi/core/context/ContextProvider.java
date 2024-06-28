package com.flipkart.fdp.superbi.core.context;

import com.flipkart.fdp.superbi.core.config.ApiKey;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 19/07/19
 */
@Slf4j
public class ContextProvider {
  private static ThreadLocal<SuperBiContext> superBiContextThreadLocal = new
      ThreadLocal<SuperBiContext>();

  public static SuperBiContext getCurrentSuperBiContext() {
    return superBiContextThreadLocal.get();
  }

  public static void setCurrentSuperBiContext(String clientId, String username, ClientPrivilege clientPrivilege){
    setCurrentSuperBiContext(clientId, username, Optional.empty(), clientPrivilege);
  }

  public static void setCurrentSuperBiContext(String clientId, String username, Optional<String> systemUser, ClientPrivilege clientPrivilege){
    try {
      superBiContextThreadLocal.set(new SuperBiContext(clientId, username,
          systemUser, clientPrivilege));
    } catch (Exception e) {
      log.error("Not able to provide superbi context");
      throw new RuntimeException(e);
    }
  }

  private String generateCacheKey(ApiKey apiKey, String userName) {
    String uniqueKey = StringUtils.join(apiKey.hashCode() + userName);
    return toMD5(uniqueKey);
  }

  private static String toMD5(String key) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      byte byteData[] = messageDigest.digest(key.getBytes());

      StringBuffer hexString = new StringBuffer();
      for (int i = 0; i < byteData.length; i++) {
        String hex = Integer.toHexString(0xff & byteData[i]);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException nsae) {
      log.error("Error while computing message digest for " +
          "key: {} error: {}", key, nsae);
      throw new RuntimeException(nsae);
    }
  }
}
