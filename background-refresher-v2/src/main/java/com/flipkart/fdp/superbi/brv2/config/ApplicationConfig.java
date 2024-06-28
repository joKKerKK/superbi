package com.flipkart.fdp.superbi.brv2.config;

import com.flipkart.fdp.superbi.d42.D42Configuration;
import com.flipkart.fdp.superbi.gcs.GcsConfig;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsConfiguration;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankConfiguration;
import com.flipkart.fdp.superbi.refresher.api.config.D42MetaConfig;
import com.flipkart.fdp.superbi.refresher.dao.druid.DruidClientConfiguration;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClientConfiguration;

import java.util.Map;

/**
 * Created by akshaya.sharma on 30/07/19
 */

public interface ApplicationConfig extends BRv2Config {

  D42Configuration getD42Configuration();

    GcsConfig getGcsConfig();

    Map<String, DataSource> getDataSourceMap();

  int getExecutorServiceThreadCount();

  GringottsConfiguration getGringottsConfiguration();

  IronBankConfiguration getIronBankConfiguration();

  D42MetaConfig getD42MetaConfig();

  DruidClientConfiguration getDruidClientConfiguration();

  FStreamClientConfiguration getFStreamClientConfiguration();
}
