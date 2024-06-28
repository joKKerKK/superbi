package com.flipkart.fdp.superbi.models;

public enum MessageQueue {
  KAFKA("KAFKA"), PUBSUB_LITE("PUBSUB_LITE");

  private String value;

  private MessageQueue(String value) {
    this.value = value;
  }
}
