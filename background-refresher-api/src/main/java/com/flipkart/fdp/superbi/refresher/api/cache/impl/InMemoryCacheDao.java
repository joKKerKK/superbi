package com.flipkart.fdp.superbi.refresher.api.cache.impl;

import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Created by akshaya.sharma on 05/07/19
 */
@Slf4j
public class InMemoryCacheDao implements CacheDao {

  private Cache<String, JsonSerializable> cache;

  public InMemoryCacheDao(int maxSize, int expireAfter, TimeUnit expireAfterUnit) {
    cache = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterAccess(expireAfter, expireAfterUnit)
        .build(new CacheLoader<String, JsonSerializable>() {
          @SneakyThrows
          public JsonSerializable load(String key) throws Exception {
            // not found in cache
            throw new RuntimeException("No result found for key : " + key);
          }
        });
  }


  @Override
  public <T extends JsonSerializable> Optional<T> get(String key,Class<T> valueClass) {
    try {
      log.info("Looking for value in cache for key " + key);
      return Optional.ofNullable((T) cache.get(key, () -> {
        // not found in cache
        log.info("value not found in cache for key " + key);
        throw new RuntimeException("No result found for key : " + key);
      }));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  @Override
  public void set(String key, int ttl, JsonSerializable v) {
    log.info("Setting value in cache for key " + key);
    cache.put(key, v);
  }

  @Override
  public void add(String k, int ttl, JsonSerializable v) {
    if (get(k,JsonSerializable.class).isPresent()) {
    } else {
      set(k, ttl, v);
    }
  }

  @Override
  public void remove(String key) {
    cache.invalidate(key);
  }
}
