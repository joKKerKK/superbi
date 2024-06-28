package com.flipkart.fdp.superbi.core.exception;

/**
 * Created by akshaya.sharma on 18/07/19
 */

public class ServiceUnavailableException extends SuperBiRuntimeException {
  public ServiceUnavailableException(String message) {
    super(message);
  }

  public ServiceUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServiceUnavailableException(Throwable cause) {
    super(cause);
  }

  public ServiceUnavailableException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
