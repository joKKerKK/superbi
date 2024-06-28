package com.flipkart.fdp.superbi.refresher.api.cache.impl.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by mansi.jain on 16/05/22
 */
@AllArgsConstructor
@Getter
@Slf4j
@Builder
public class BigTableClient {

  private BigtableDataClient bigtableDataClient;

  private String tableId;

}
