package com.flipkart.fdp.superbi.core.exception;

/**
 * Created by akshaya.sharma on 31/07/19
 */

public class MalformedReportException extends RuntimeException {
  public MalformedReportException() {
    this("Report is malformed!");
  }

  public MalformedReportException(String message) {
    super(message);
  }

  public MalformedReportException(String message, Throwable cause) {
    super(message, cause);
  }

  public MalformedReportException(Throwable cause) {
    super(cause);
  }

  public MalformedReportException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
