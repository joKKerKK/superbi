package com.flipkart.fdp.superbi.subscription.configurations;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class SubscriptionJobConfig {
  private long backOffTimeInMillis;

  @JsonCreator
  public SubscriptionJobConfig(@JsonProperty("backOffTimeInMillis") long backOffTimeInMillis) {
    this.backOffTimeInMillis = backOffTimeInMillis;
  }
}
