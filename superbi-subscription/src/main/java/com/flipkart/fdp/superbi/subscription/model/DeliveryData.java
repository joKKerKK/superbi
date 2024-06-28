package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "deliveryAction",
    visible = true)
@JsonSubTypes({
    @Type(value = EmailDelivery.class, name = "EMAIL"),
    @Type(value = FTPDelivery.class, name = "FTP"),
    @Type(value = FTPDelivery.class, name = "SFTP"),
})
public abstract class DeliveryData {

  public enum DeliveryAction {EMAIL,FTP, SFTP}

  DeliveryAction deliveryAction;

  public DeliveryData(DeliveryAction deliveryAction) {
    this.deliveryAction = deliveryAction;
  }
}
