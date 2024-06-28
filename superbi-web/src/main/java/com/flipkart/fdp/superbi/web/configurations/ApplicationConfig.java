package com.flipkart.fdp.superbi.web.configurations;

import com.flipkart.fdp.superbi.core.config.ApiKey;
import com.flipkart.fdp.superbi.core.config.SuperbiConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.BadgerClientConfiguration;
import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.http.client.qaas.QaasConfiguration;
import com.flipkart.fdp.superbi.refresher.api.config.D42MetaConfig;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClientConfiguration;
import com.flipkart.fdp.superbi.web.exception.ExceptionInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by akshaya.sharma on 30/07/19
 */

public interface ApplicationConfig extends SuperbiConfig {
  Map<String, ExceptionInfo> getExceptionInfoMap();
  D42Configuration getD42Configuration();
  Map<String, DataSource> getDataSourceMap();
  int getExecutorServiceThreadCount();
  Map<String, Object> getKafkaProducerConfig();
  List<ApiKey> getApiKeys();
  D42MetaConfig getD42MetaConfig();
  DruidClientConfiguration getDruidClientConfiguration();
  boolean getFactRefreshTimeRequired(String dataSource);
  FStreamClientConfiguration getFStreamClientConfiguration();
  BadgerClientConfiguration getBadgerClientConfiguration();
  QaasConfiguration getQaasConfiguration();
}
