package com.flipkart.fdp.superbi.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class FTPDelivery extends DeliveryData{

  private String host;
  private String port;
  private String userName;
  private String password;
  private String ftpLocation;
  private List<String> commEmails;

  @JsonCreator
  public FTPDelivery(
      @JsonProperty("deliveryAction") DeliveryAction deliveryAction, @JsonProperty("host")String host,
      @JsonProperty("port") String port, @JsonProperty("userName")String userName,
      @JsonProperty("password") String password, @JsonProperty("ftpLocation")String ftpLocation,
      @JsonProperty("commEmails") List<String> commEmails) {
    super(deliveryAction);
    this.host = host;
    this.port = port;
    this.userName = userName;
    this.password = password;
    this.ftpLocation = ftpLocation;
    this.commEmails = commEmails;
  }
}
