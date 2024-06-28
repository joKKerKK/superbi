package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;

/**
 * Created by amruth.s on 01/10/15.
 */
public interface CosmosQueryAuditer {
  void audit(ExecutorQueryInfoLog log);
}
