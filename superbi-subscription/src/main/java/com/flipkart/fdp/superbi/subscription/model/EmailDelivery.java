package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmailDelivery extends DeliveryData{
  List<String> subscribers;

  @JsonCreator
  public EmailDelivery(@JsonProperty("subscribers") List<String> subscribers,
      @JsonProperty("deliveryAction") DeliveryAction deliveryAction) {
    super(deliveryAction);
    this.subscribers = subscribers;
  }
}
