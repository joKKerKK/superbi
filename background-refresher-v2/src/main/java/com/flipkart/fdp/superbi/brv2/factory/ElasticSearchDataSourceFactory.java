package com.flipkart.fdp.superbi.brv2.factory;

import com.flipkart.fdp.es.client.ESClient;
import com.flipkart.fdp.es.client.ESClientConfig;
import com.flipkart.fdp.es.client.http.ESHttpClient;
import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.elastic.ElasticSearchDataSourceDao;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;

public class ElasticSearchDataSourceFactory implements DataSourceFactory {

    private static final String DEFAULT_FETCH_SIZE = "100";
    private static final String DEFAULT_REQ_TIMEOUT_MS = "120000";
    private static final String DEFAULT_SCROLL_BATCH_SIZE = "1024";
    private static final String DEFAULT_SCROLL_ALIVE_TIME = "1m";


    @Override
    public DataSourceDao getDao(Map<String, Object> attributes) {
        final String host = (String) attributes.get("host");
        final Optional<String> clusterName = Optional.fromNullable((String) attributes.get("clusterName"));
        Preconditions.checkNotNull(host);
        return new ElasticSearchDataSourceDao(getEsClient(host,clusterName,attributes));
    }

    @Override
    public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
        return DataSourceType.ELASTIC_SEARCH.getDslConfig(dslConfig);
    }

    private static ESClient getEsClient(final String host,
                                       Optional<String> clusterNameOptional,
                                       final Map<String, Object> attributes){
        return new ESHttpClient(ESClientConfig.builder()
                .hosts(Arrays.asList(host.split(",")))
                .clusterName(clusterNameOptional.or(""))
                .requestTimeoutMs(
                        Integer.parseInt((String) attributes.getOrDefault("requestTimeoutMs", DEFAULT_REQ_TIMEOUT_MS))
                )
                .termFetchSize(Integer.parseInt((String) attributes.getOrDefault("termFetchSize", DEFAULT_FETCH_SIZE)))
                .scrollBatchSize(Integer.parseInt((String) attributes.getOrDefault("scrollBatchSize", DEFAULT_SCROLL_BATCH_SIZE)))
                .scrollAliveTime((String) attributes.getOrDefault("scrollAliveTime", DEFAULT_SCROLL_ALIVE_TIME))
                .build()
        );
    }
}
