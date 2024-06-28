package com.flipkart.fdp.superbi.execution;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.dao.lock.LockDao;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class DefaultbackgroundRefresherTest {

  @Mock
  private Function<String, BackgroundRefresherConfig> backgroundRefresherConfigProvider;

  @Mock
  private BackgroundRefreshTaskExecutor taskExecutor;

  @Mock
  private RetryTaskHandler retryTaskHandler;

  @Mock
  private LockDao lockDao;

  public static final List<SelectColumn> selectColumns = Lists.newArrayList(
      ExprFactory.SEL_COL("event_name").as("event_name").selectColumn,
      new SelectColumn.SimpleColumn("business_unit", "business_unit"),
      ExprFactory.AGGR("gmv", AggregationType.SUM).as("gmv").selectColumn
  );
  public static final String temp_fact_1 = "test_gmv_unit_target_fact";

  private static DSQuery dsQuery = DSQuery.builder()
      .withColumns(selectColumns)
      .withFrom(temp_fact_1)
      .withGroupByColumns(Lists.newArrayList("event_name", "business_unit"))
      .withLimit(1)
      .build();

  private static final QueryPayload SAMPLE_QUERY_PAYLOAD = new QueryPayload("VERTICA_DEFAULT",
      "attemptKey", "cacheKey", 0, null, 0, "", "", "", dsQuery, new HashMap<>(), new HashMap<>(),null);

  private static final BackgroundRefresherConfig BACKGROUND_REFRESHER_CONFIG = new BackgroundRefresherConfig(
      2, 3, 4, 5, 6, 7, 7, 8, 2, 0.0d,1024);


  @Test
  public void testSubmitQuery() {
    DefaultBackgroundRefresher defaultBackgroundRefresher = new DefaultBackgroundRefresher(lockDao,
        backgroundRefresherConfigProvider, taskExecutor, retryTaskHandler);
    Mockito.when(backgroundRefresherConfigProvider.apply(Mockito.any()))
        .thenReturn(BACKGROUND_REFRESHER_CONFIG);
    defaultBackgroundRefresher.submitQuery(SAMPLE_QUERY_PAYLOAD);
    verify(taskExecutor, times(1)).executeTaskAsync(Mockito.any());
  }
}
