package com.flipkart.fdp.superbi.subscription.configurations;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DefaultEmailConfig {
  private String userName;
  private String password;
  private String host;

  public DefaultEmailConfig(@JsonProperty("userName") String userName,
      @JsonProperty("password") String password, @JsonProperty("host") String host) {
    this.userName = userName;
    this.password = password;
    this.host = host;
  }
}
