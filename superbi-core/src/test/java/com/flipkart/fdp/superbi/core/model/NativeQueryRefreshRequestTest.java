package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.es.client.ESQuery.QueryType;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidQuery;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.Aggregate;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.AggregateType;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FStreamQuery;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.Granularity;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.Range;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class NativeQueryRefreshRequestTest {

  @Test
  public void testSerializationAndDeserializationForSQLNativeQuery() {
    NativeQueryRefreshRequest nativeQueryRefreshRequest = getSQLQuery();
    String serializedRefreshRequest = JsonUtil.toJson(nativeQueryRefreshRequest);
    NativeQueryRefreshRequest deSerializedRefreshRequest = JsonUtil.fromJson(
        serializedRefreshRequest, NativeQueryRefreshRequest.class);
    Assert.assertEquals(deSerializedRefreshRequest, nativeQueryRefreshRequest);
  }

  @Test
  public void testSerializationAndDeserializationForDruidNativeQuery() {
    NativeQueryRefreshRequest nativeQueryRefreshRequest = getDruidQuery();
    String serializedRefreshRequest = JsonUtil.toJson(nativeQueryRefreshRequest);
    NativeQueryRefreshRequest deSerializedRefreshRequest = JsonUtil.fromJson(
        serializedRefreshRequest, NativeQueryRefreshRequest.class);
    Assert.assertEquals(deSerializedRefreshRequest, nativeQueryRefreshRequest);
  }

  @Test
  public void testSerializationAndDeserializationForESNativeQuery() {
    NativeQueryRefreshRequest nativeQueryRefreshRequest = getESQuery();
    String serializedRefreshRequest = JsonUtil.toJson(nativeQueryRefreshRequest);
    NativeQueryRefreshRequest deSerializedRefreshRequest = JsonUtil.fromJson(
        serializedRefreshRequest, NativeQueryRefreshRequest.class);
    Assert.assertEquals(deSerializedRefreshRequest, nativeQueryRefreshRequest);
  }

  @Test
  public void testSerializationAndDeserializationForFstreamNativeQuery() {
    NativeQueryRefreshRequest nativeQueryRefreshRequest = getFstreamQuery();
    String serializedRefreshRequest = JsonUtil.toJson(nativeQueryRefreshRequest);
    NativeQueryRefreshRequest deSerializedRefreshRequest = JsonUtil.fromJson(
        serializedRefreshRequest, NativeQueryRefreshRequest.class);
    Assert.assertEquals(deSerializedRefreshRequest, nativeQueryRefreshRequest);
  }

  private NativeQueryRefreshRequest getSQLQuery() {
    return getNativeQueryRefreshRequest(new NativeQuery("select * from A"));
  }

  private NativeQueryRefreshRequest getDruidQuery() {
    List<String> headerList = Arrays.asList("platform", "visitors");
    return getNativeQueryRefreshRequest(new NativeQuery(new DruidQuery(
        "select \"platform\" as platform, \"visitors\" as visitors from euclid_ooo_stream where TIMESTAMP_TO_MILLIS(__time)>=1577817000000 and TIMESTAMP_TO_MILLIS(__time)<1609439400000 limit 10000",
        headerList, new HashMap<>())));
  }

  private NativeQueryRefreshRequest getESQuery() {
    ObjectNode queryNode = JsonUtil.fromJson(
        "{\"timeout\":\"180000ms\",\"aggs\":{\"filter_wrapper\":{\"filter\":{\"match_all\":{}},\"aggs\":{\"unit_creation_timestamp\":{\"max\":{\"field\":\"unit_creation_timestamp\"}}}}},\"size\":0}",
        ObjectNode.class);
    return getNativeQueryRefreshRequest(
        new NativeQuery(
        com.flipkart.fdp.superbi.refresher.dao.elastic.requests.ESQuery.builder()
            .index("f_cp_pnl--pnl_live_fact")
            .type("_doc")
            .queryType(com.flipkart.fdp.es.client.ESQuery.QueryType.TERM)
            .schema(ImmutableList.copyOf(Lists.newArrayList("unit_creation_timestamp")))
            .columns(ImmutableList.copyOf(Lists.newArrayList("unit_creation_timestamp")))
            .queryType(QueryType.valueOf("AGGR"))
            .limit(10000)
            .query(queryNode)
            .build()));
  }

  private NativeQueryRefreshRequest getFstreamQuery() {
    List<String> orderedColumnList = Arrays.asList("_ts", "ver", "dc", "ing_src", "tot", "fal",
        "byt_in");
    FStreamQuery fStreamQuery = new FStreamQuery(new FstreamRequest(Lists.newArrayList("cf._ts",
        "cf.ver", "cf.ing_src", "cf.dc"), Lists.newArrayList("cf._ts"), null,
        new Range(1644258600000L, 1644863400000L,
            Granularity.builder().value("daily").build()),
        Lists.newArrayList(
            Aggregate.builder()
                .fieldName("cf.tot")
                .aggregateType(AggregateType.builder()
                    .value("SUM")
                    .build())
                .build(),

            Aggregate.builder()
                .fieldName("cf.fal")
                .aggregateType(AggregateType.builder()
                    .value("SUM")
                    .build())
                .build(),

            Aggregate.builder()
                .fieldName("cf.byt_in")
                .aggregateType(AggregateType.builder()
                    .value("SUM")
                    .build())
                .build()
        ), null), "34", orderedColumnList);

    return getNativeQueryRefreshRequest(new NativeQuery(fStreamQuery));
  }

  private NativeQueryRefreshRequest getNativeQueryRefreshRequest(NativeQuery nativeQuery) {
    return NativeQueryRefreshRequest.builder()
        .storeIdentifier("Target_DEFAULT")
        .fromTable("table")
        .deadLine(10000)
        .cacheKey("test_cache_key")
        .executionEngineLabels(new HashMap<>())
        .appliedFilters(new HashMap<>())
        .nativeQuery(nativeQuery)
        .build();
  }
}
