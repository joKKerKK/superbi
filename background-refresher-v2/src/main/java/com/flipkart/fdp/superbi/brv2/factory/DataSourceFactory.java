package com.flipkart.fdp.superbi.brv2.factory;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import java.util.Map;

public interface DataSourceFactory {

    DataSourceDao getDao(Map<String, Object> attributes);

    AbstractDSLConfig getDslConfig(Map<String, String> dslConfig);
}
