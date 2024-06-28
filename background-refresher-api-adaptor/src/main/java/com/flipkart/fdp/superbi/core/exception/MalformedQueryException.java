package com.flipkart.fdp.superbi.core.exception;

public class MalformedQueryException extends RuntimeException {
  public MalformedQueryException(String message) {
    super(message);
  }

  public MalformedQueryException(Throwable cause) {
    super(cause);
  }
}
