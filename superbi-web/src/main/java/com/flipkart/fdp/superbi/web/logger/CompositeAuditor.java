package com.flipkart.fdp.superbi.web.logger;

import com.flipkart.fdp.superbi.core.config.SuperbiConfig;
import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.model.AuditInfo;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Created by akshaya.sharma on 29/08/19
 */
@Slf4j
public class CompositeAuditor implements Auditer, ExecutionAuditor {

  private final SuperbiConfig superbiConfig;
  private final List<Auditer> auditers;

  @Inject
  public CompositeAuditor(@Named("AUDITERS") List<Auditer> auditers, SuperbiConfig superbiConfig) {
    this.auditers = auditers;
    this.superbiConfig = superbiConfig;
  }


  @Override
  public void audit(QueryInfo queryInfo) {
    if (isAuditorEnabled()) {
      auditers.forEach(auditer -> {
        try {
          auditer.audit(queryInfo);
        }catch (Exception e) {
          log.error("Auditer error : ", e);
        }
      });
    }
  }

  @Override
  public void audit(AuditInfo auditInfo) {
    if (isAuditorEnabled()) {
      auditers.forEach(auditer -> {
        try {
          auditer.audit(auditInfo);
        }catch (Exception e) {
          log.error("Auditer error : ", e);
        }
      });
    }
  }

  @Override
  public void audit(ExecutorQueryInfoLog queryInfoLog) {
    if (isAuditorEnabled()) {
      auditers.forEach(auditer -> {
        try {
          auditer.audit(queryInfoLog);
        }catch (Exception e) {
          log.error("Auditer error : ", e);
        }
      });
    }
  }

  @Override
  public boolean isAuditorEnabled() {
    return superbiConfig.isSuperBiAuditerEnabled();
  }

  @Override
  public void audit(ExecutionLog executionLog) {
    if (isAuditorEnabled()) {
      auditers.forEach(auditer -> {
        try {
          if(auditer instanceof ExecutionAuditor) {
            ((ExecutionAuditor) auditer).audit(executionLog);
          }
        }catch (Exception e) {
          log.error("Auditer error : ", e);
        }
      });
    }
  }
}
