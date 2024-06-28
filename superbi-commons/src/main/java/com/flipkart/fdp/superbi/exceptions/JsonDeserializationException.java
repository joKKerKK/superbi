package com.flipkart.fdp.superbi.exceptions;

public class JsonDeserializationException extends RuntimeException {

  public JsonDeserializationException() {
  }

  public JsonDeserializationException(String message) {
    super(message);
  }

  public JsonDeserializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public JsonDeserializationException(Throwable cause) {
    super(cause);
  }
}