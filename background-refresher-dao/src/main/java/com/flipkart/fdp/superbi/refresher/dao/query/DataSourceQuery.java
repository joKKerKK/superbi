package com.flipkart.fdp.superbi.refresher.dao.query;

import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class DataSourceQuery {
  private Object nativeQuery;
  private String cacheKey;
  private MetaDataPayload metaDataPayload;
}
