package com.flipkart.fdp.superbi.core.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by akshaya.sharma on 19/07/19
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class ApiKey {
  @JsonProperty
  private final String clientId;
  @JsonProperty
  private final String clientSecret;
}
