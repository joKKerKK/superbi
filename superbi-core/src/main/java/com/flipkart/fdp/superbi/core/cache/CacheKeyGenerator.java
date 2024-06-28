package com.flipkart.fdp.superbi.core.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import com.flipkart.fdp.superbi.core.model.QueryRefreshRequest;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by akshaya.sharma on 18/06/19
 */

@Slf4j
public abstract class CacheKeyGenerator<Q extends QueryRefreshRequest> {
  public abstract String getCacheKey(Q queryRefreshRequest, String sourceIdentifier);

  public static String toMD5(String key) {
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