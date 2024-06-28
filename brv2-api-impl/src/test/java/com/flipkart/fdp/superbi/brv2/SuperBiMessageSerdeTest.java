package com.flipkart.fdp.superbi.brv2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
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
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

public class SuperBiMessageSerdeTest extends BackgroundRefresherTestSuite {

  private final SuperBiMessageSerializer serializer = new SuperBiMessageSerializer();
  private final SuperBiMessageDeserializer deserializer = new SuperBiMessageDeserializer();

  @Test
  public void testSerializationAndDeserialization() {
    long currentTime = System.currentTimeMillis();
    SuperBiMessage superBiMessage = new SuperBiMessage(3, currentTime,
        10000, queryPayload);
    byte[] bytes = serializer.serialize("test", superBiMessage);
    SuperBiMessage deserializedMessage = deserializer.deserialize("test", bytes);
    assertSuperbiMessages(superBiMessage, deserializedMessage);
  }

  @Test
  public void testSerializationAndDeserializationForSQLNativeQuery() {
    long currentTime = System.currentTimeMillis();
    SuperBiMessage superBiMessage = new SuperBiMessage(3, currentTime,
        10000, getSQLQueryPayload());
    byte[] bytes = serializer.serialize("test", superBiMessage);
    SuperBiMessage deserializedMessage = deserializer.deserialize("test", bytes);
    assertSuperbiMessages(superBiMessage, deserializedMessage);
  }

  @Test
  public void testSerializationAndDeserializationForDruidNativeQuery() {
    long currentTime = System.currentTimeMillis();
    SuperBiMessage superBiMessage = new SuperBiMessage(3, currentTime,
        10000, getDruidQueryPayload());
    byte[] bytes = serializer.serialize("test", superBiMessage);
    SuperBiMessage deserializedMessage = deserializer.deserialize("test", bytes);
    assertSuperbiMessages(superBiMessage, deserializedMessage);
  }

  @Test
  public void testSerializationAndDeserializationForESNativeQuery() {
    long currentTime = System.currentTimeMillis();
    SuperBiMessage superBiMessage = new SuperBiMessage(3, currentTime,
        10000, getESQueryPayload());
    byte[] bytes = serializer.serialize("test", superBiMessage);
    SuperBiMessage deserializedMessage = deserializer.deserialize("test", bytes);
    assertSuperbiMessages(superBiMessage, deserializedMessage);
  }

  @Test
  public void testSerializationAndDeserializationForFstreamNativeQuery() {
    long currentTime = System.currentTimeMillis();
    SuperBiMessage superBiMessage = new SuperBiMessage(3, currentTime,
        10000, getFstreamQueryPayload());
    byte[] bytes = serializer.serialize("test", superBiMessage);
    SuperBiMessage deserializedMessage = deserializer.deserialize("test", bytes);
    assertSuperbiMessages(superBiMessage, deserializedMessage);
  }

  private QueryPayload getSQLQueryPayload() {
    return getQueryPayload("select * from A");
  }

  private QueryPayload getDruidQueryPayload() {
    return getQueryPayload(new DruidQuery("query", new ArrayList<>(), new HashMap<>()));
  }

  @SneakyThrows
  private QueryPayload getESQueryPayload() {
    ObjectNode queryNode = JsonUtil.fromJson("{\"a\": \"b\"}", ObjectNode.class);
    return getQueryPayload(com.flipkart.fdp.superbi.refresher.dao.elastic.requests.ESQuery.builder()
        .query(JsonUtil.fromJson("{}", ObjectNode.class))
        .queryType(com.flipkart.fdp.es.client.ESQuery.QueryType.TERM)
        .schema(ImmutableList.copyOf(Lists.newArrayList()))
        .columns(ImmutableList.copyOf(Lists.newArrayList()))
        .query(queryNode)
        .build());
  }

  private QueryPayload getFstreamQueryPayload() {
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

    return getQueryPayload(fStreamQuery);
  }

  private QueryPayload getQueryPayload(Object nativeQuery) {
    return QueryPayload.builder()
        .dsQuery(dsQuery)
        .priority("default")
        .params(Maps.newHashMap())
        .storeIdentifier("Target_DEFAULT")
        .deadLine(10000)
        .queryWeight(1)
        .clientId("TEST")
        .cacheKey(TEST_CACHE_KEY)
        .nativeQuery(new NativeQuery(nativeQuery))
        .build();
  }

  private void assertSuperbiMessages(SuperBiMessage expected, SuperBiMessage value) {
    Assert.assertEquals(expected.getRemainingRetries(), value.getRemainingRetries());
    Assert.assertEquals(expected.getQueryPayload(), value.getQueryPayload());
    Assert.assertEquals(expected.getBackoffInMillis(), value.getBackoffInMillis());
    Assert.assertEquals(expected.getExecuteAfter(), value.getExecuteAfter());
  }
}
