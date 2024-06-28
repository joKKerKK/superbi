package com.flipkart.fdp.superbi.core.logger;

import com.flipkart.fdp.superbi.core.model.AuditInfo;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.cosmos.meta.api.CosmosQueryAuditer;

/**
 * Created by akshaya.sharma on 29/08/19
 */

public interface Auditer extends CosmosQueryAuditer {
  boolean isAuditorEnabled();
  void audit(AuditInfo auditInfo);
  void audit(QueryInfo queryInfo);
}
