package com.flipkart.fdp.superbi.subscription.exceptions;

public class ClientSideException extends RuntimeException{

  public ClientSideException(String message) {
    super(message);
  }

  public ClientSideException(Throwable th) {
    super(th);
  }

  public ClientSideException(String message, Throwable th) {
    super(message, th);
  }
}
