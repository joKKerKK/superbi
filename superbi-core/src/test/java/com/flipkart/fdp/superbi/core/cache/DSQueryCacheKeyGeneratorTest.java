package com.flipkart.fdp.superbi.core.cache;


import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.LiteralEvalExp;
import com.flipkart.fdp.superbi.dsl.query.predicate.EqualsPredicate;
import com.flipkart.fdp.superbi.dsl.query.predicate.InPredicate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DSQueryCacheKeyGeneratorTest {

  private DSQueryCacheKeyGenerator dsQueryCacheKeyGenerator = new DSQueryCacheKeyGenerator();


  @Test
  public void testCacheKeyDifferenceWithSameDsQuery() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertTrue(cacheKey.equals(anotherCacheKey));
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentCriteria() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        EqualsPredicate(new ColumnExp("column2"), new LiteralEvalExp("value2"));
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentInCriteria() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        InPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        InPredicate(new ColumnExp("column2"), new LiteralEvalExp("value2"));
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentCriteriaValue() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value2"));
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentParams() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value2"));
    Map<String, String[]> anotherParams = new HashMap<>();
    params.put("forward_unit_live_fact.brand_id$operator$", param);
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, anotherParams, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentSelectColumn() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    SelectColumn.SimpleColumn anotherColumn = new SelectColumn.SimpleColumn("col2",
        "alias");
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(anotherColumn)).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentGroupByColumn() {

    String columnExp = "groupByOne";
    String anotherColumnExp = "groupByTwo";

    DSQuery dsQuery = DSQuery.builder().withGroupByColumns(Arrays.asList(columnExp))
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    DSQuery anotherDsQuery = DSQuery.builder().withGroupByColumns(Arrays.asList(anotherColumnExp)).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentTable() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table1").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table2").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey.equals(anotherCacheKey));
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentOrderByColumn() {

    String columnExp = "orderOne";
    String anotherColumnExp = "orderTwo";

    DSQuery dsQuery = DSQuery.builder().withOrderByColumns(Arrays.asList(columnExp))
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    DSQuery anotherDsQuery = DSQuery.builder().withOrderByColumns(Arrays.asList(anotherColumnExp)).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey == anotherCacheKey);
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentLimit() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);

    DSQuery.Builder dsQueryBuilder = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table");
    DSQuery dsQuery = dsQueryBuilder.withLimit(200).build();
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    DSQuery anotherDsQuery = dsQueryBuilder.withLimit(20).build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey.equals(anotherCacheKey));
  }

  @Test
  public void testCacheKeyDifferenceWithOneLimitAbsent() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);

    DSQuery.Builder dsQueryBuilder = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table");
    DSQuery dsQuery = dsQueryBuilder.build();
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    DSQuery anotherDsQuery = dsQueryBuilder.withLimit(200).build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertFalse(cacheKey.equals(anotherCacheKey));
  }

  @Test
  public void testCacheKeyDifferenceWithBothLimitAbsent() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);

    DSQuery.Builder dsQueryBuilder = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table");
    DSQuery dsQuery = dsQueryBuilder.build();
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    DSQuery anotherDsQuery = dsQueryBuilder.build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "Druid_DEFAULT");
    Assert.assertTrue(cacheKey.equals(anotherCacheKey));
  }

  @Test
  public void testCacheKeyDifferenceWithDifferentStoreIdentifier() {

    SelectColumn.SimpleColumn column = new SelectColumn.SimpleColumn("col",
        "alias");
    Criteria criteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery dsQuery = DSQuery.builder().withColumns(Arrays.asList(column)).withCriteria(criteria)
        .withFrom("table").build();
    Map<String, String[]> params = new HashMap<>();
    String[] param = {"eq"};
    params.put("forward_unit_live_fact.seller_id$operator$", param);
    String cacheKey = dsQueryCacheKeyGenerator.getCacheKey(dsQuery, params, "Druid_DEFAULT");
    Criteria anotherCriteria = new
        EqualsPredicate(new ColumnExp("column"), new LiteralEvalExp("value"));
    DSQuery anotherDsQuery = DSQuery.builder().withColumns(Arrays.asList(column))
        .withCriteria(anotherCriteria).withFrom("table").build();
    String anotherCacheKey = dsQueryCacheKeyGenerator
        .getCacheKey(anotherDsQuery, params, "ElasticSearch2_DEFAULT");
    Assert.assertFalse(cacheKey.equals(anotherCacheKey));
  }
}
