package com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mansi.jain on 12/05/22
 */
@AllArgsConstructor
@Getter
@Slf4j
@Builder
public class BigTableConfig {
  private String projectId;
  private String instanceId;

  @SneakyThrows
  public final BigtableDataClient getBigTableDataClient(){
    Credentials credentials = ServiceAccountCredentials.getApplicationDefault();
    BigtableDataSettings settings =
        BigtableDataSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .setProjectId(projectId).setInstanceId(instanceId).build();
    return BigtableDataClient.create(settings);
  }
}
