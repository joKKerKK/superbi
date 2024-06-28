package com.flipkart.fdp.superbi.web.logger;


import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.model.AuditInfo;
import com.flipkart.fdp.superbi.core.model.QueryInfo;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionAuditor;
import com.flipkart.fdp.superbi.refresher.dao.audit.ExecutionLog;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;


/**
 * Created by akshaya.sharma on 01/08/19
 */
@Slf4j
public class AuditFileLogger implements Auditer, ExecutionAuditor {

  @Override
  public boolean isAuditorEnabled() {
    // File logger is always enabled
    return true;
  }

  @Override
  public void audit(AuditInfo auditInfo) {
    MDC.put("code", "AuditInfo");
    log.info(convertToMessage(auditInfo));
  }

  @Override
  public void audit(QueryInfo queryInfo) {
    MDC.put("code", "QueryInfo");
    log.info(convertToMessage(queryInfo));
  }

  @Override
  public void audit(ExecutorQueryInfoLog logData) {
    MDC.put("code", "ExecutorQueryInfoLog");
    log.info(convertToMessage(logData));
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
