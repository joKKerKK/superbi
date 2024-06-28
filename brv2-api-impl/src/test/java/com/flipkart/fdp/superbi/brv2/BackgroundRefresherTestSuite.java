package com.flipkart.fdp.superbi.brv2;

import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;

public abstract class BackgroundRefresherTestSuite {
  protected static final String TEST_CACHE_KEY = "test_key";
  protected final List<SelectColumn> selectColumns = Lists.newArrayList(
      ExprFactory.SEL_COL("event_name").as("event_name").selectColumn,
      new SelectColumn.SimpleColumn("business_unit", "business_unit"),
      ExprFactory.AGGR("gmv", AggregationType.SUM).as("gmv").selectColumn
  );
  protected final String temp_fact_1 = "test_gmv_unit_target_fact";
  protected DSQuery dsQuery = DSQuery.builder()
      .withColumns(selectColumns)
      .withFrom(temp_fact_1)
      .withGroupByColumns(Lists.newArrayList("event_name", "business_unit"))
      .withLimit(1)
      .build();

  protected QueryPayload queryPayload = QueryPayload.builder()
      .dsQuery(dsQuery)
      .priority("default")
      .params(Maps.newHashMap())
      .storeIdentifier("Target_DEFAULT")
      .deadLine(10000)
      .queryWeight(1)
      .clientId("TEST")
      .cacheKey(TEST_CACHE_KEY)
      .nativeQuery(new NativeQuery(""))
      .build();
}
