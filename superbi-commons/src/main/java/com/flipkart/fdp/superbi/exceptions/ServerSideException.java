package com.flipkart.fdp.superbi.exceptions;

/**
 * Created by akshaya.sharma on 31/12/19
 */

public class ServerSideException extends SuperBiException {

  public ServerSideException(String message){
    super(message);
  }

  public ServerSideException(Throwable cause){
    super(cause);
  }

  public ServerSideException(String message, Throwable cause) {
    super(message, cause);
  }
}
