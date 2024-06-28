package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.Builder;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScheduleInfo {

  public static final String SUPERBI_DOWNLOAD_PREFIX = "superbi_download_";

  public enum ResourceType{REPORT, CANVAS}

  public enum DeliveryType{CSV,D42,GSHEET}

  private final String org;
  private final String namespace;
  private final String reportName;
  private final String widgets;
  private final String ownerId;
  private final String subscriptionName;
  private final long subscriptionId;

  private final DeliveryData deliveryData;
  private final ResourceType resourceType;
  private final DeliveryType deliveryType;
  private final Map<String, List<String>> params;
  private final Map<String, String> deliveryConfig;
  private Long triggerTime;
  private Long nextFireTime;
  private Integer attempt;
  private final String scheduleRunId;
  private final String requestId;
  private final Boolean isOTS;
  private final String cron;

  @JsonCreator
  @Builder
  public ScheduleInfo(@JsonProperty("org") String org, @JsonProperty("namespace") String namespace,
      @JsonProperty("reportName") String reportName, @JsonProperty("ownerId") String ownerId,
      @JsonProperty("resourceType") ResourceType resourceType,
      @JsonProperty("deliveryType") DeliveryType deliveryType,
      @JsonProperty("params") Map<String, List<String>> params,
      @JsonProperty("subscriptionName") String subscriptionName,
      @JsonProperty("subscriptionId") long subscriptionId,
      @JsonProperty("DeliveryData") DeliveryData deliveryData,
      @JsonProperty("deliveryConfig") Map<String, String> deliveryConfig,
      @JsonProperty("triggerTime") Long triggerTime,
      @JsonProperty("scheduleDeliveryId") String scheduleRunId,
      @JsonProperty("attempt")Integer attempt,
      @JsonProperty("nextFireTime")Long nextFireTime,
      @JsonProperty("cron") String cron,
                      @JsonProperty("widgets") String widgets) {
    this.org = org;
    this.namespace = namespace;
    this.reportName = reportName;
    this.ownerId = ownerId;
    this.subscriptionName = subscriptionName;
    this.resourceType = resourceType;
    this.deliveryType = deliveryType;
    this.params = params;
    this.subscriptionId = subscriptionId;
    this.deliveryData = deliveryData;
    this.deliveryConfig = deliveryConfig;
    this.attempt = attempt == null ? 0 : attempt;
    this.triggerTime = triggerTime;
    this.scheduleRunId = scheduleRunId != null ? scheduleRunId : generateRandomUuid();
    this.requestId = generateRandomUuid();
    this.isOTS = subscriptionName.contains(SUPERBI_DOWNLOAD_PREFIX);
    this.nextFireTime = nextFireTime;
    this.cron = cron;
    this.widgets = widgets;
  }

  private static String generateRandomUuid() {
    Random rnd = ThreadLocalRandom.current();
    long mostSig = rnd.nextLong();
    long leastSig = rnd.nextLong();
    mostSig &= -61441L;
    mostSig |= 16384L;
    leastSig &= 4611686018427387903L;
    leastSig |= -9223372036854775808L;
    return new UUID(mostSig, leastSig).toString();
  }

}
