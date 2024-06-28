package com.flipkart.fdp.superbi.execution;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.execution.exception.DataSourceNotFoundException;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog.ExecutionLogBuilder;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;


@Slf4j
@AllArgsConstructor
public class BasicQueryExecutor {

    private static final String DATASOURCE_METRIC_KEY = "execution.datasource.time";
    private static final String ACTIVE_STREAM_KEY = "stream.active";
    private static final String DATASOURCE_EXECUTION_METER_KEY = "execution.datasource";

    private final Map<String, DataSourceDao> daoProvider;

    private final MetricRegistry metricRegistry;

    @SneakyThrows
    public QueryResult execute(String storeIdentifier, DataSourceQuery dataSourceQuery,
        ExecutionLogBuilder executionLogBuilder){

        getMeterForQueryExecution(storeIdentifier).mark();

        try(Timer.Context context = getTimerForQueryExecution(storeIdentifier)){

            log.info("executing query for storeIdentifier {}, query - {}",storeIdentifier,dataSourceQuery.getNativeQuery());
            DataSourceDao dataSourceDao = daoProvider.get(storeIdentifier);

            if(dataSourceDao == null) {
                throw new DataSourceNotFoundException(MessageFormat.format(
                        "Data source not provided in map for store identifier {0} ", storeIdentifier));
            }
            try {
                QueryResult queryResult =  dataSourceDao.getStream(dataSourceQuery);
                return new MetricQueryResult(queryResult,storeIdentifier);
            }finally {
                long elapsedTime = context.stop();
                executionLogBuilder.sourceName(storeIdentifier).executionTimeMs(elapsedTime/1000000);
            }
        }
    }

    private Timer.Context getTimerForQueryExecution(String storeIdentifier) {
        return metricRegistry.timer(getMetricsKey(DATASOURCE_METRIC_KEY,storeIdentifier)).time();
    }

    private Meter getMeterForQueryExecution(String storeIdentifier) {
        return metricRegistry.meter(getMetricsKey(DATASOURCE_EXECUTION_METER_KEY,storeIdentifier));
    }

    private String getMetricsKey(String prefix,String storeIdentifier) {
        return StringUtils.join(Arrays.asList(prefix,storeIdentifier),'.');
    }

    private class MetricQueryResult extends QueryResult{

        private QueryResult queryResult;
        private Counter counter;
        private boolean isStreamCLosed;

        MetricQueryResult(QueryResult queryResult, String storeIdentifier) {
            this.queryResult = queryResult;
            this.counter = getCounterForMetricResult(storeIdentifier);
            this.counter.inc();
            this.isStreamCLosed = false;
        }

        @Override
        public Iterator<List<Object>> iterator() {
            return queryResult.iterator();
        }

        @Override
        public List<String> getColumns() {
            return queryResult.getColumns();
        }

        @Override
        public void close() {
            if(!isStreamCLosed){
              queryResult.close();
              counter.dec();
              isStreamCLosed = true;
            }
        }

        private Counter getCounterForMetricResult(String storeIdentifier){
            return metricRegistry.counter(getMetricsKey(ACTIVE_STREAM_KEY,storeIdentifier));
        }
    }

}
