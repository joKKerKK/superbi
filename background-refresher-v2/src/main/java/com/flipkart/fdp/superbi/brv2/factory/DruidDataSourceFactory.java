package com.flipkart.fdp.superbi.brv2.factory;

import com.flipkart.fdp.superbi.cosmos.DataSourceType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClient;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidDataSourceDao;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DruidDataSourceFactory implements DataSourceFactory{

  private final DruidClient druidClient;

  @Override
  public DataSourceDao getDao(Map<String, Object> attributes) {
    String host = (String) attributes.get("host");
    String port = (String) attributes.get("port");
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(port);
    return new DruidDataSourceDao(new DruidClient(druidClient.getAsyncHttpClient(), host, Integer.parseInt(port),
        druidClient.getClientSideExceptions()));
  }

  @Override
  public AbstractDSLConfig getDslConfig(Map<String, String> dslConfig) {
    return DataSourceType.DRUID.getDslConfig(dslConfig);
  }

}
