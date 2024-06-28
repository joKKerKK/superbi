package com.flipkart.fdp.superbi.execution;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.execution.exception.DataSourceNotFoundException;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.collect.Lists;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class BasicQueryExecutorTest {
    
    @Mock
    private DataSourceDao dataSourceDao;

    private static String SAMPLE_QUERY = "select * from forward_unit_vertica_fact";

    private DataSourceQuery dataSourceQuery = DataSourceQuery.builder().nativeQuery(SAMPLE_QUERY)
        .cacheKey("cache_key").build();

    private static ExecutionLog.ExecutionLogBuilder superBiExecutionLogBuilder = ExecutionLog.builder();

    private static List<List<Object>> totalResult = Collections.singletonList(Arrays.asList(1, 2, 3, 4));

    private static List<String> columnNames = Lists.newArrayList("event_name", "business_unit", "gmv");

    @Test
    public void testSuccessQueryCase(){
        QueryResult queryResult = new QueryResult() {
            Iterator<List<Object>> resultIterator = totalResult.iterator();
            @Override
            public Iterator<List<Object>> iterator() {
                return resultIterator;
            }

            @Override
            public List<String> getColumns() {
                return columnNames;
            }

            @Override
            public void close() {

            }
        };
        Mockito.when(dataSourceDao.getStream(Mockito.any())).thenReturn(queryResult);

        Map<String,DataSourceDao> daoMap = new HashMap<>();
        String storeIdentifier = "VERTICA_DEFAULT";
        daoMap.put(storeIdentifier,dataSourceDao);

        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(20).minimumNumberOfCalls(2).build();
        Map<String,CircuitBreakerConfig> circuitBreakerConfigMap = new HashMap<>();
        circuitBreakerConfigMap.put(storeIdentifier,circuitBreakerConfig);

        BulkheadConfig bulkheadConfig = BulkheadConfig.custom().maxConcurrentCalls(200)
                .build();
        Map<String,BulkheadConfig> bulkheadConfigMap = new HashMap<>();
        bulkheadConfigMap.put(storeIdentifier,bulkheadConfig);

        BasicQueryExecutor queryExecutor = new BasicQueryExecutor(daoMap,new MetricRegistry());

        QueryResult storeQueryResult = queryExecutor.execute(storeIdentifier,dataSourceQuery,superBiExecutionLogBuilder);

        Assert.assertTrue(queryResult.iterator().hasNext());
        Assert.assertEquals(storeQueryResult.iterator().next(),Arrays.asList(1,2,3,4));
        Assert.assertFalse(queryResult.iterator().hasNext());
        Assert.assertEquals(storeQueryResult.getColumns(),queryResult.getColumns());
    }

    @Test(expected = DataSourceNotFoundException.class)
    public void testDataSourceNotFound(){
        Map<String,DataSourceDao> daoMap = new HashMap<>();
        final String storeIdentifier = "VERTICA_DEFAULT";
        final String otherStoreIndetifier = "ELASTICSEARCH2_DEFAULT";
        daoMap.put(storeIdentifier,dataSourceDao);

        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(20).minimumNumberOfCalls(2).build();
        Map<String,CircuitBreakerConfig> circuitBreakerConfigMap = new HashMap<>();
        circuitBreakerConfigMap.put(storeIdentifier,circuitBreakerConfig);

        BulkheadConfig bulkheadConfig = BulkheadConfig.custom().maxConcurrentCalls(200)
                .build();
        Map<String,BulkheadConfig> bulkheadConfigMap = new HashMap<>();
        bulkheadConfigMap.put(storeIdentifier,bulkheadConfig);
        BasicQueryExecutor queryExecutor = new BasicQueryExecutor(daoMap,new MetricRegistry());

        queryExecutor.execute(otherStoreIndetifier,dataSourceQuery,superBiExecutionLogBuilder);
    }
}
