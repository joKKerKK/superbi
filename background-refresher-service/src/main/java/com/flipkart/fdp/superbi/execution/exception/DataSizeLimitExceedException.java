package com.flipkart.fdp.superbi.execution.exception;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;

public class DataSizeLimitExceedException extends
    ClientSideException {

  public DataSizeLimitExceedException(String message) {
    super(message);
  };
}