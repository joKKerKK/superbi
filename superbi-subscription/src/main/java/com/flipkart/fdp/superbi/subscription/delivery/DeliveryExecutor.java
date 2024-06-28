package com.flipkart.fdp.superbi.subscription.delivery;

import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import java.util.Date;
import java.util.List;

public interface DeliveryExecutor {

  void sendContent(ReportDataResponse reportDataResponse,ScheduleInfo scheduleInfo);

  void sendFailureContent(ScheduleInfo scheduleInfo,Exception e,List<String> subscribers);

  void sendSubscriptionExpiryComm(ScheduleInfo scheduleInfo,List<String> receipients, Date expiryDate);

}
