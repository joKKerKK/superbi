package com.flipkart.fdp.superbi.brv2.logger;

import com.flipkart.fdp.superbi.brv2.config.BRv2Config;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompositeAuditor implements Auditor, ExecutionAuditor {

  private final BRv2Config bRv2Config;
  private final List<Auditor> auditors;

  @Inject
  public CompositeAuditor(BRv2Config bRv2Config,
      @Named("AUDITORS") List<Auditor> auditors) {
    this.bRv2Config = bRv2Config;
    this.auditors = auditors;
  }


  @Override
  public boolean isAuditorEnabled() {
    return bRv2Config.isAuditorEnabled();
  }

  @Override
  public void audit(ExecutionLog executionLog) {
    if (isAuditorEnabled()) {
      auditors.forEach(auditor -> {
        try {
          if (auditor instanceof ExecutionAuditor) {
            ((ExecutionAuditor) auditor).audit(executionLog);
          }
        } catch (Exception e) {
          log.error("Auditor error : ", e);
        }
      });
    }
  }
}
