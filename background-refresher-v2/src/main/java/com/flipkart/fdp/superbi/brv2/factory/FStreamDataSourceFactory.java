package com.flipkart.fdp.superbi.brv2.factory;

import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClient;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamDataSourceDao;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;

import java.util.Map;

@AllArgsConstructor
public class FStreamDataSourceFactory implements DataSourceFactory {
    private final FStreamClient fStreamClient;


    @Override
    public DataSourceDao getDao(Map<String, Object> attributes) {
        String host = (String) attributes.get("host");
        String port = (String) attributes.get("port");
        Preconditions.checkNotNull(host);
        Preconditions.checkNotNull(port);
        return new FStreamDataSourceDao(fStreamClient);
    }

    @Override
    public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
        return DataSourceType.FSTREAM.getDslConfig(dslConfig);
    }
}
