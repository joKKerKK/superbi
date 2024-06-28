package com.flipkart.fdp.superbi.core.adaptor.translators;

import static com.flipkart.fdp.superbi.dsl.query.factory.CriteriaFactory.EQ;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.COL;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.LIT;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.PARAM;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaCreator;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaModifier;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by akshaya.sharma on 04/07/19
 */
@Slf4j
@Ignore
public class VerticaQueryBuilderTest {
  private VerticaDSLConfig verticaDSLConfig = new VerticaDSLConfig(Maps.newHashMap());
  private final List<SelectColumn> selectColumns = Lists.newArrayList(
      ExprFactory.SEL_COL("event_name").as("event_name").selectColumn,
      new SelectColumn.SimpleColumn("business_unit", "business_unit"),
      ExprFactory.AGGR("gmv", AggregationType.SUM).as("gmv").selectColumn
  );
  private final String temp_fact_1 = "test_gmv_unit_target_fact";

  @SneakyThrows
  public VerticaQueryBuilderTest() {
    Map overrides = Maps.newHashMap();
    overrides.put("hibernate.current_session_context_class",
        "org.hibernate.context.internal.ThreadLocalSessionContext");

    EntityManagerFactory entityManagerFactory = new HibernatePersistenceProvider()
        .createEntityManagerFactory(
            "TEST_COSMOS", overrides);
    SessionFactory sessionFactory = entityManagerFactory.unwrap(SessionFactory.class);


    MetaCreator.initialize(sessionFactory);
    MetaModifier.initialize(sessionFactory);
    MetaAccessor.initialize(sessionFactory);
  }

  @Before
  public void setUp() throws Exception {

  }

  private final AbstractQueryBuilder getTranslator(DSQuery query,
      Map<String, String[]> paramValues) {
    return new VerticaQueryBuilder(query, paramValues, verticaDSLConfig);
  }


  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void simpleSelectWithLimitAndNoParams() {
    String expectedSql = "select event_name as 'event_name',business_unit as 'business_unit',sum"
        + "(gmv) as 'gmv' from b_bigfoot_test.test_gmv_unit_target_fact limit 1";
    DSQuery dsQuery = DSQuery.builder()
        .withColumns(selectColumns)
        .withFrom(temp_fact_1)
        .withLimit(1)
        .build();

    AbstractQueryBuilder queryBuilder = getTranslator(dsQuery, Maps.newHashMap());

    Object nativeQuery = queryBuilder.buildQuery();
    log.info(nativeQuery.toString());

    Assert.assertEquals(expectedSql, nativeQuery);
  }

  @Test
  public void simpleSelectWithGroupByAndNoParams() {
    String expectedSql = "select event_name as 'event_name',business_unit as 'business_unit',sum"
        + "(gmv) as 'gmv' from b_bigfoot_test.test_gmv_unit_target_fact group by event_name,"
        + "business_unit limit 1";
    DSQuery dsQuery = DSQuery.builder()
        .withColumns(selectColumns)
        .withGroupByColumns(Lists.newArrayList("event_name", "business_unit"))
        .withFrom(temp_fact_1)
        .withLimit(1)
        .build();

    AbstractQueryBuilder queryBuilder = getTranslator(dsQuery, Maps.newHashMap());

    Object nativeQuery = queryBuilder.buildQuery();
    log.info(nativeQuery.toString());

    Assert.assertEquals(expectedSql, nativeQuery);
  }

  @Test
  public void simpleSelectWithStaticWhere() {
    String expectedSql = "select event_name as 'event_name',business_unit as 'business_unit',sum"
        + "(gmv) as 'gmv' from b_bigfoot_test.test_gmv_unit_target_fact where event_name "
        + "='base_event' group by business_unit limit 1";

    Criteria criteria = EQ(COL("event_name"), LIT("base_event"));

    DSQuery dsQuery = DSQuery.builder()
        .withColumns(selectColumns)
        .withGroupByColumns(Lists.newArrayList("business_unit"))
        .withFrom(temp_fact_1)
        .withCriteria(criteria)
        .withLimit(1)
        .build();

    AbstractQueryBuilder queryBuilder = getTranslator(dsQuery, Maps.newHashMap());

    Object nativeQuery = queryBuilder.buildQuery();
    log.info(nativeQuery.toString());

    Assert.assertEquals(expectedSql, nativeQuery);
  }

  @Test
  public void simpleSelectWithWhereAndParams() {
    String expectedSql = "select event_name as 'event_name',business_unit as 'business_unit',sum"
        + "(gmv) as 'gmv' from b_bigfoot_test.test_gmv_unit_target_fact where event_name "
        + "='base_event' group by business_unit limit 1";

    Criteria criteria = EQ(COL("event_name"), PARAM("from.base_event", DataType.STRING));

    DSQuery dsQuery = DSQuery.builder()
        .withColumns(selectColumns)
        .withGroupByColumns(Lists.newArrayList("business_unit"))
        .withFrom(temp_fact_1)
        .withCriteria(criteria)
        .withLimit(1)
        .build();

    Map<String, String[]> params = Maps.newHashMap();
    params.put("from.base_event", new String[] {"base_event"});

    AbstractQueryBuilder queryBuilder = getTranslator(dsQuery, params);

    Object nativeQuery = queryBuilder.buildQuery();
    log.info(nativeQuery.toString());

    Assert.assertEquals(expectedSql, nativeQuery);
  }
}
