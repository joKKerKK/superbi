package com.flipkart.fdp.superbi.refresher.api.result.query;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 12/07/19
 */
@AllArgsConstructor
@Getter
public class QueryResultLocation{
  private final String link;
  private final long linkExpiry;
  private final long createdAt;
  private final String creator;
}
