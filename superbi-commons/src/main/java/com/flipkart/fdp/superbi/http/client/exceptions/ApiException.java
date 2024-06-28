package com.flipkart.fdp.superbi.http.client.exceptions;

public class ApiException extends RuntimeException{

  public ApiException(Throwable cause) {
    super(cause);
  }

  public ApiException(String message) {
    super(message);
  }

}
