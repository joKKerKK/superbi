package com.flipkart.fdp.superbi.web.util;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Created by akshaya.sharma on 09/07/19
 */

public class MapUtil {
//  public static <K, V> Map<K, V[]> convertToMap(MultivaluedMap<K, V> orig, V[] out) {
//  TODO guava multi version issue
//    Set<K> keySet = orig.keySet();
//
//    V[] vs = Arrays.copyOfRange(out, 0, 0);
//
//    java.util.function.Function<K, V @Nullable []> transform =
//        new Function<K, V[]>() {
//          @Override
//          public V[] apply(@NotNull K key) {
//            return orig.get(key).toArray(vs);
//          }
//        };
//
//    return Maps.asMap(keySet, transform::apply);
//  }

  public static Map<String, String[]> convertToMap(MultivaluedMap<String, String> multivaluedMap) {
    if (multivaluedMap == null) {
      return null;
    }

    Map<String, String[]> result = new HashMap<>();
    multivaluedMap.entrySet().forEach(entry -> {
      result.put(entry.getKey(), entry.getValue().toArray(new String[] {}));
    });

    return result;
  }
}
