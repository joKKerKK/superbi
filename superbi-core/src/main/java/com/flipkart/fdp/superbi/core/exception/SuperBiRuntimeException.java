package com.flipkart.fdp.superbi.core.exception;

import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import java.util.Optional;

/**
 * Created by akshaya.sharma on 19/07/19
 */

public class SuperBiRuntimeException extends RuntimeException {

  private Optional<QueryResultCachedValue> cachedData = Optional.empty();

  public SuperBiRuntimeException() {
  }

  public SuperBiRuntimeException(String message) {
    super(message);
  }

  public SuperBiRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public SuperBiRuntimeException(Throwable cause) {
    super(cause);
  }

  public SuperBiRuntimeException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }


  public SuperBiRuntimeException(String message, Throwable cause, QueryResultCachedValue cachedData) {
    super(message, cause);
    this.cachedData = Optional.ofNullable(cachedData);
  }

  public SuperBiRuntimeException( Throwable cause, QueryResultCachedValue cachedData) {
    super(cause);
    this.cachedData = Optional.ofNullable(cachedData);;
  }

  public Optional<QueryResultCachedValue> getCachedData() {
    return cachedData;
  }

}
