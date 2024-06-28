package com.flipkart.fdp.superbi.brv2.logger;


import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;


/**
 * Created by akshaya.sharma on 01/08/19
 */
@Slf4j
public class AuditFileLogger implements Auditor, ExecutionAuditor {

  @Override
  public boolean isAuditorEnabled() {
    // File logger is always enabled
    return true;
  }

  private static String convertToMessage(Object object) {
    if (object == null) {
      return "";
    }
    return JsonUtil.toJson(object);
  }

  @Override
  public void audit(ExecutionLog executionLog) {
    MDC.put("code", "ExecutionLog");
    log.info(convertToMessage(executionLog));
  }
}
