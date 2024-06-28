package com.flipkart.fdp.superbi.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class MapUtil {

  public static Map<String,String> stringToMap(String s)  {
    Map<String, String> attributes = new HashMap<String, String>();

    Properties p = new Properties();
    try {
      p.load(new StringReader(s));
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (Object key : p.keySet()) {
      attributes.put(key.toString(), p.get(key).toString().trim());
    }
    return attributes;
  }

  public static String MapToString(Map<String,String> map)
  {
    return map.toString();
  }
}
