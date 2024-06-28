package com.flipkart.fdp.superbi.subscription.configurations;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class ApiAuthConfig {

  private String clientId;
  private String clientSecret;

  @JsonCreator
  public ApiAuthConfig( @JsonProperty("clientId") String clientId,
      @JsonProperty("clientSecret") String clientSecret) {
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }
}
