package com.flipkart.fdp.superbi.brv2.logger;

import com.flipkart.fdp.superbi.brv2.model.AuditInfo;
import com.flipkart.fdp.superbi.brv2.model.QueryInfo;

/**
 * Created by akshaya.sharma on 29/08/19
 */

public interface Auditor {
  boolean isAuditorEnabled();
}
