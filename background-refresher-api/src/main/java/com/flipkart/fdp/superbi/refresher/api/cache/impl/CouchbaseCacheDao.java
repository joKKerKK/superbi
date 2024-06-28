package com.flipkart.fdp.superbi.refresher.api.cache.impl;

import com.codahale.metrics.*;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import com.flipkart.fdp.superbi.refresher.api.exception.CanNotReadCacheException;
import com.flipkart.fdp.superbi.refresher.api.exception.CanNotRemoveCacheException;
import com.flipkart.fdp.superbi.refresher.api.exception.CanNotSetCacheException;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;

/**
 * Created by akshaya.sharma on 22/07/19
 */
@Slf4j
public class CouchbaseCacheDao implements CacheDao {

  private final Bucket bucket;
  private final MetricRegistry metricRegistry;
  private static final String HIT_METER_KEY = "couchbase.hit.meter";
  private static final String GET_TIMER_KEY = "couchbase.get.time";
  private static final String HIT_RATIO_KEY = "couchbase.hitratio";
  private static final String ADD_METER_KEY = "couchbase.add.meter";
  private static final String SET_METER_KEY = "couchbase.set.meter";
  private static final String ERROR_METER_KEY = "couchbase.error.meter";
  private final Meter cacheHitMeter;
  private final Timer cacheGetTimer;
  private final Meter cacheAddMeter;
  private final Meter cacheSetMeter;
  private final Meter cacheErrorMeter;
  private final Gauge<Double> cacheHitRatio;


    public CouchbaseCacheDao(Bucket bucket, MetricRegistry metricRegistry) {
        this.bucket = bucket;
        this.metricRegistry = metricRegistry;
        this.cacheHitMeter = metricRegistry.meter(getMetricsKey(HIT_METER_KEY,bucket.name()));
        this.cacheGetTimer = metricRegistry.timer(getMetricsKey(GET_TIMER_KEY,bucket.name()));
        this.cacheAddMeter = metricRegistry.meter(getMetricsKey(ADD_METER_KEY,bucket.name()));
        this.cacheSetMeter = metricRegistry.meter(getMetricsKey(SET_METER_KEY,bucket.name()));
        this.cacheErrorMeter = metricRegistry.meter(getMetricsKey(ERROR_METER_KEY,bucket.name()));
        this.cacheHitRatio = metricRegistry.gauge(getMetricsKey(HIT_RATIO_KEY,bucket.name()),()-> (Gauge) () -> RatioGauge.Ratio.of(cacheHitMeter.getOneMinuteRate(),
            cacheGetTimer.getOneMinuteRate()).getValue());

    }

    @Override
    public <T extends JsonSerializable> Optional<T> get(String key,Class<T> valueClass) {

      try (Timer.Context context = cacheGetTimer.time()){

        Optional<T> value = Optional.ofNullable(bucket.get(key, RawJsonDocument.class))
            .map(AbstractDocument::content)
            .map(json -> JsonUtil.fromJson(json, valueClass));

        if(!value.isPresent()) {
          log.info("Cache miss for key '{}'", key);
        }else {
          cacheHitMeter.mark();
        }

        return value;
      } catch (Exception ex) {
        cacheErrorMeter.mark();
        log.error("Can not get data for key '{}", key);
        throw new CanNotReadCacheException(ex);
      }
  }

  public void set(String key, int ttl, JsonSerializable value) {
    try {
      bucket.upsert(
          RawJsonDocument.create(key, ttl, JsonUtil.toJson(value))
      );
      cacheSetMeter.mark();
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Cannot set the key '{}' with value '{}' to cache: {}", key, value, ex);
      throw new CanNotSetCacheException(ex);
    }
  }


  public void add(String key, int ttl, JsonSerializable value) {
    try {
      bucket.insert(RawJsonDocument.create(key, ttl, JsonUtil.toJson(value))
      );
      cacheAddMeter.mark();
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Cannot add the key '{}' with value '{}' to cache: {}", key, value, ex);
      throw new CanNotSetCacheException(ex);
    }
  }


  public void remove(String key) {
    try {
      bucket.remove(key);
    } catch (DocumentDoesNotExistException doesNotExistException) {
      log.info("Can not remove, No document exist for key '{}'", key);
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Not able to remove key: {} in cache error: {}", key, ex);
      throw new CanNotRemoveCacheException(ex);
    }
  }

  private static String getMetricsKey(String prefix, String buckName){
      return StringUtils.join(Arrays.asList(prefix,buckName),'.');
  }
}