package com.flipkart.fdp.superbi.web.factory;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;

import java.util.Map;

public interface DataSourceFactory {

    DataSourceDao getDao(Map<String, String> attributes);

    AbstractDSLConfig getDslConfig(Map<String,String> dslConfig);
}
