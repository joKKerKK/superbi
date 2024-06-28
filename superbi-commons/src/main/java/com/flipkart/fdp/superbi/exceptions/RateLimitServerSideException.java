package com.flipkart.fdp.superbi.exceptions;

/**
 * Created by nitin.chauhan1 on 28/09/23
 */

public class RateLimitServerSideException extends SuperBiException {

  public RateLimitServerSideException(String message){
    super(message);
  }

  public RateLimitServerSideException(Throwable cause){
    super(cause);
  }

  public RateLimitServerSideException(String message, Throwable cause) {
    super(message, cause);
  }
}
