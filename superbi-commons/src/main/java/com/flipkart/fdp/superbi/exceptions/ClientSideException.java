package com.flipkart.fdp.superbi.exceptions;

public class ClientSideException extends SuperBiException {
  public ClientSideException(String message){
    super(message);
  }

  public ClientSideException(Throwable e){
    super(e);
  }

  public ClientSideException(String message, Throwable e){
    super(message,e);
  }
}
