package com.flipkart.fdp.superbi.core.exception;

import com.flipkart.fdp.auth.common.exception.AuthorizationException;

public class UnMappedUserException extends AuthorizationException {

  public UnMappedUserException(String message) {
    super(message);
  }
}
