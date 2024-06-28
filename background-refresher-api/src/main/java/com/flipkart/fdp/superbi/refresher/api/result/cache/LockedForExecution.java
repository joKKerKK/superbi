package com.flipkart.fdp.superbi.refresher.api.result.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 19/06/19
 */
@Getter
public class LockedForExecution implements JsonSerializable{

  private final long cachedAtTime;
  private final String resultKey;

  @Builder
  @JsonCreator
  public LockedForExecution(@JsonProperty("cachedAtTime") long cachedAtTime,
      @JsonProperty("resultKey") String resultKey) {
    this.cachedAtTime = cachedAtTime;
    this.resultKey = resultKey;
  }
}