package com.flipkart.fdp.superbi.refresher.api.cache;

import java.util.Optional;

/**
 * Created by akshaya.sharma on 18/06/19
 */

public interface CacheDao {

  <T extends JsonSerializable> Optional<T> get(String k,Class<T> valueClass);

  void set(String k, int ttl, JsonSerializable v);

  void add(String k, int ttl, JsonSerializable v);

  void remove(String k);
}
