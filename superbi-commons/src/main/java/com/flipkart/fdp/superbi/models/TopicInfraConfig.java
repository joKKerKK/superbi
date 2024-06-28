package com.flipkart.fdp.superbi.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TopicInfraConfig {

  private String cloudRegion;
  private Character cloudZoneId;
  private Long projectId;

  private boolean isBatchEnabled;
  private Long elementCountThreshold;
  private Long requestByteThreshold;
  private Long delayThreshold;

}
