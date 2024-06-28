package com.flipkart.fdp.superbi.refresher.api.cache.impl;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable.BigTableClient;
import com.flipkart.fdp.superbi.refresher.api.exception.CanNotReadCacheException;
import com.flipkart.fdp.superbi.refresher.api.exception.CanNotRemoveCacheException;
import com.flipkart.fdp.superbi.refresher.api.exception.CanNotSetCacheException;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by mansi.jain on 11/05/22
 */
@Slf4j
public class BigTableCacheDao implements CacheDao {

  private final BigtableDataClient bigtableDataClient;
  private final MetricRegistry metricRegistry;
  private final String tableId;
  private static final String HIT_METER_KEY = "bigtable.hit.meter";
  private static final String GET_TIMER_KEY = "bigtable.get.time";
  private static final String HIT_RATIO_KEY = "bigtable.hitratio";
  private static final String ADD_METER_KEY = "bigtable.add.meter";
  private static final String SET_METER_KEY = "bigtable.set.meter";
  private static final String ERROR_METER_KEY = "bigtable.error.meter";
  private final Meter cacheHitMeter;
  private final Timer cacheGetTimer;
  private final Meter cacheAddMeter;
  private final Meter cacheSetMeter;
  private final Meter cacheErrorMeter;
  private final Gauge<Double> cacheHitRatio;
  private static final String FAMILY_NAME = "result";
  private static final String QUALIFIER = "value";

  public BigTableCacheDao(BigTableClient bigTableClient,
      MetricRegistry metricRegistry) {
    this.bigtableDataClient = bigTableClient.getBigtableDataClient();
    this.metricRegistry = metricRegistry;
    this.tableId = bigTableClient.getTableId();
    this.cacheHitMeter = metricRegistry.meter(getMetricsKey(HIT_METER_KEY, tableId));
    this.cacheGetTimer = metricRegistry.timer(getMetricsKey(GET_TIMER_KEY, tableId));
    this.cacheAddMeter = metricRegistry.meter(getMetricsKey(ADD_METER_KEY, tableId));
    this.cacheSetMeter = metricRegistry.meter(getMetricsKey(SET_METER_KEY, tableId));
    this.cacheErrorMeter = metricRegistry.meter(getMetricsKey(ERROR_METER_KEY, tableId));
    this.cacheHitRatio = metricRegistry.gauge(getMetricsKey(HIT_RATIO_KEY, tableId),
        () -> (Gauge) () -> RatioGauge.Ratio.of(cacheHitMeter.getOneMinuteRate(),
            cacheGetTimer.getOneMinuteRate()).getValue());
  }


  @Override
  public <T extends JsonSerializable> Optional<T> get(String k, Class<T> valueClass) {
    try (Timer.Context context = cacheGetTimer.time()) {

      Filter filter = FILTERS.limit().cellsPerRow(1);

      Optional<T> value = Optional.ofNullable(bigtableDataClient.readRow(tableId, k, filter))
          .map(row -> row.getCells().get(0)).map(cell -> cell.getValue().toStringUtf8())
          .map(json -> JsonUtil.fromJson(json, valueClass));

      if (!value.isPresent()) {
        log.info("Cache miss for key '{}'", k);
      } else {
        cacheHitMeter.mark();
      }
      return value;
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Can not get data for key '{}'", k);
      throw new CanNotReadCacheException(ex);
    }
  }

  @Override
  public void set(String k, int ttl, JsonSerializable v) {
    try {
      RowMutation rowMutation = RowMutation.create(tableId, k)
          .setCell(FAMILY_NAME, QUALIFIER, JsonUtil.toJson(v));
      bigtableDataClient.mutateRow(rowMutation);
      cacheSetMeter.mark();
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Cannot add the key '{}' with value '{}' to cache: {}", k, v, ex);
      throw new CanNotSetCacheException(ex);
    }
  }

  @Override
  public void add(String k, int ttl, JsonSerializable v) {
    try {
      RowMutation rowMutation = RowMutation.create(tableId, k)
          .setCell(FAMILY_NAME, QUALIFIER, JsonUtil.toJson(v));
      bigtableDataClient.mutateRow(rowMutation);
      cacheAddMeter.mark();
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Cannot add the key '{}' with value '{}' to cache: {}", k, v, ex);
      throw new CanNotSetCacheException(ex);
    }
  }

  @Override
  public void remove(String k) {
    try {
      bigtableDataClient.mutateRow(RowMutation.create(tableId, k).deleteRow());
    } catch (Exception ex) {
      cacheErrorMeter.mark();
      log.error("Not able to remove key: {} in cache error: {}", k, ex);
      throw new CanNotRemoveCacheException(ex);
    }
  }

  private static String getMetricsKey(String prefix, String tableId) {
    return StringUtils.join(Arrays.asList(prefix, tableId), '.');
  }
}
