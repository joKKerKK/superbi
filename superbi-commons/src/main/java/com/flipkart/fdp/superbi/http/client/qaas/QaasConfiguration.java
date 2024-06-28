package com.flipkart.fdp.superbi.http.client.qaas;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class QaasConfiguration {
  private final String host;
  private final String clientId;
  private final String clientSecret;
  private final String context;
  private final Long blockingTimeout;
  private final String downloadEndPoint;
  private final long connectTimeout;
  private final long readTimeout;
}
