package com.flipkart.fdp.superbi.web.exception;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.eclipse.jetty.http.HttpStatus;

/**
 * Created by akshaya.sharma on 30/07/19
 */
@Getter
@Builder
@AllArgsConstructor
public class ExceptionInfo {
  private final int statusCode;
  private final String errorCode;
  private final String errorMessage;
  private final boolean allowMessageForward;
}
