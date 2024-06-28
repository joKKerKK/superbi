package com.flipkart.fdp.superbi.refresher.dao.exceptions;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;

public class DataSizeLimitExceedException extends
    ClientSideException {

  public DataSizeLimitExceedException(String message) {
    super(message);
  };
}