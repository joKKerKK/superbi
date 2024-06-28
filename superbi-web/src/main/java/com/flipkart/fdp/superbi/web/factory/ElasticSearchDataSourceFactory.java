package com.flipkart.fdp.superbi.web.factory;

import com.flipkart.fdp.es.client.ESClient;
import com.flipkart.fdp.es.client.ESClientConfig;
import com.flipkart.fdp.es.client.http.ESHttpClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.elastic.ElasticSearchDataSourceDao;

import java.util.Arrays;
import java.util.Map;

public class ElasticSearchDataSourceFactory implements DataSourceFactory {

    private static final String DEFAULT_FETCH_SIZE = "100";
    private static final String DEFAULT_REQ_TIMEOUT_MS = "120000";
    private static final String DEFAULT_SCROLL_BATCH_SIZE = "1024";
    private static final String DEFAULT_SCROLL_ALIVE_TIME = "1m";


    @Override
    public DataSourceDao getDao(Map<String, String> attributes) {
        final String host = attributes.get("host");
        final Optional<String> clusterName = Optional.fromNullable(attributes.get("clusterName"));
        Preconditions.checkNotNull(host);
        return new ElasticSearchDataSourceDao(getEsClient(host,clusterName,attributes));
    }

    @Override
    public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
        return new ElasticParserConfig(dslConfig);
    }

    private static ESClient getEsClient(final String host,
                                       Optional<String> clusterNameOptional,
                                       final Map<String, String> attributes){
        return new ESHttpClient(ESClientConfig.builder()
                .hosts(Arrays.asList(host.split(",")))
                .clusterName(clusterNameOptional.or(""))
                .requestTimeoutMs(
                        Integer.parseInt(attributes.getOrDefault("requestTimeoutMs", DEFAULT_REQ_TIMEOUT_MS))
                )
                .termFetchSize(Integer.parseInt(attributes.getOrDefault("termFetchSize", DEFAULT_FETCH_SIZE)))
                .scrollBatchSize(Integer.parseInt(attributes.getOrDefault("scrollBatchSize", DEFAULT_SCROLL_BATCH_SIZE)))
                .scrollAliveTime(attributes.getOrDefault("scrollAliveTime", DEFAULT_SCROLL_ALIVE_TIME))
                .build()
        );
    }
}
