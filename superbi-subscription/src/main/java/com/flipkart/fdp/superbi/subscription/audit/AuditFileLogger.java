package com.flipkart.fdp.superbi.subscription.audit;

import com.flipkart.fdp.superbi.subscription.model.audit.ScheduleInfoLog;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
@NoArgsConstructor
public class AuditFileLogger implements Auditor {

  @Override
  public boolean isAuditorEnabled() {
    return true;
  }

  @Override
  public void audit(ScheduleInfoLog scheduleInfoLog) {
    MDC.put("code", "ScheduleInfoAudit");
    log.info(convertToMessage(scheduleInfoLog));
  }

  private static String convertToMessage(Object object) {
    if (object == null) {
      return "";
    }
    return JsonUtil.toJson(object);
  }
}
