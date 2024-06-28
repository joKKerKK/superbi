package com.flipkart.fdp.superbi.web.factory;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.druid.DruidDSLConfig;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClient;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidDataSourceDao;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DruidDataSourceFactory implements DataSourceFactory {
  private final DruidClient druidClient;

  @Override
  public DataSourceDao getDao(Map<String, String> attributes) {
    String host = attributes.get("host");
    String port = attributes.get("port");
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(port);
    return new DruidDataSourceDao(druidClient);
  }

  @Override
  public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
    return new DruidDSLConfig(dslConfig);
  }

}
