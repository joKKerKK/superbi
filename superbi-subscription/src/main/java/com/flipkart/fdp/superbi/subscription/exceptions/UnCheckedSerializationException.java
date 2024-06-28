package com.flipkart.fdp.superbi.subscription.exceptions;


public class UnCheckedSerializationException extends RuntimeException{
  public UnCheckedSerializationException() {
  }

  public UnCheckedSerializationException(String message) {
    super(message);
  }

  public UnCheckedSerializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnCheckedSerializationException(Throwable cause) {
    super(cause);
  }
}