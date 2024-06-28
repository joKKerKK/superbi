package com.flipkart.fdp.superbi.exceptions;

/**
 * Created by: mansi.jain on 08/09/23.
 */
public class InvalidQueryException extends RuntimeException{

  public InvalidQueryException(String message){
    super(message);
  }

  public InvalidQueryException(Throwable e){
    super(e);
  }

  public InvalidQueryException(String message, Throwable e){
    super(message,e);
  }

}
