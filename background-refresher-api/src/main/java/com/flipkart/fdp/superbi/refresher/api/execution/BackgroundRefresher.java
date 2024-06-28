package com.flipkart.fdp.superbi.refresher.api.execution;

/**
 * Created by akshaya.sharma on 18/06/19
 */

public interface BackgroundRefresher {
  void submitQuery(final QueryPayload queryPayload);
}
