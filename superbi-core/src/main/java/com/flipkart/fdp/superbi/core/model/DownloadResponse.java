package com.flipkart.fdp.superbi.core.model;

import com.codahale.metrics.annotation.Metered;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Getter
public class DownloadResponse extends FetchQueryResponse {

  private final String url;
  private final String service;
  private final boolean isRedirect;

  @Builder
  @Metered
  public DownloadResponse(Map<String, String> appliedFilters, String url, String service, boolean isRedirect) {
    super(appliedFilters);
    this.url = url;
    this.service = service;
    this.isRedirect = isRedirect;
  }
}
