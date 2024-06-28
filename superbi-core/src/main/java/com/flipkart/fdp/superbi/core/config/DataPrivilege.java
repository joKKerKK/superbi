package com.flipkart.fdp.superbi.core.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DataPrivilege {
  private Integer limit = 100000;
  private LimitPriority limitPriority = LimitPriority.REPORT;
  private Boolean fullDataClient = false;

  public enum LimitPriority {
    SOURCE,CLIENT,REPORT, CONFIG,MIN
  }
}
