package com.flipkart.fdp.superbi.brv2.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class DedupeStoreConfig {
  private final int maxSize;
  private final int expiryTimeInMin;
}
