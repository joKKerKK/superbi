package com.flipkart.fdp.superbi.exceptions;

public class SuperBiException extends RuntimeException{

  public SuperBiException(String message){
    super(message);
  }

  public SuperBiException(Throwable e){
    super(e);
  }

  public SuperBiException(String message, Throwable e){
    super(message,e);
  }

}
