package com.flipkart.fdp.superbi.web.exception;

import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.google.inject.Inject;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import java.util.Map;
import java.util.function.Supplier;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpStatus;

/**
 * Created by akshaya.sharma on 30/07/19
 */

@Provider
@Slf4j
public class SuperBiExceptionMapper implements ExceptionMapper<Exception> {

  //  static Map<String, ExceptionInfo> exceptionInfoMap = Maps.newHashMap();
  private final Supplier<Map<String, ExceptionInfo>> exceptionInfoSupplier;

  @Inject
  public SuperBiExceptionMapper(Supplier<Map<String, ExceptionInfo>> exceptionInfoSupplier) {
    this.exceptionInfoSupplier = exceptionInfoSupplier;
  }

  public static final String INTERNAL_SERVER_ERROR_MESSAGE = "INTERNAL_SERVER_ERROR";

  private Map<String, ExceptionInfo> getExceptionInfoMap() {
    return exceptionInfoSupplier.get();
  }

  private static ExceptionInfo getExceptionInfo(Throwable exception,
      Map<String, ExceptionInfo> exceptionInfoMap) {

    if (exceptionInfoMap.containsKey(exception.getClass().getCanonicalName())) {
      return exceptionInfoMap.get(exception.getClass().getCanonicalName());
    } else {
      if (exception instanceof HystrixRuntimeException) {
        return handleHysterixRuntimeException((HystrixRuntimeException) exception,
            exceptionInfoMap);
      } else if (exception instanceof RuntimeException) {
        return handleRuntimeException((RuntimeException) exception, exceptionInfoMap);
      } else {
        return getUnHandledExceptionInfo(exception);
      }

    }
  }

  private static ExceptionInfo handleHysterixRuntimeException(HystrixRuntimeException exception,
      Map<String, ExceptionInfo> exceptionInfoMap) {
    Throwable reason = exception.getCause() != null ? exception.getCause() : exception;
    if (exceptionInfoMap.containsKey(reason.getClass().getCanonicalName())) {
      return exceptionInfoMap.get(reason.getClass().getCanonicalName());
    } else {
      return getUnHandledExceptionInfo(reason);
    }
  }

  private static ExceptionInfo getUnHandledExceptionInfo(Throwable exception) {
    log.error("Received unhandled exception ", exception);
    return ExceptionInfo.builder()
        .allowMessageForward(false)
        .errorMessage(INTERNAL_SERVER_ERROR_MESSAGE)
        .statusCode(HttpStatus.Code.INTERNAL_SERVER_ERROR.getCode())
        .build();
  }

  private static ExceptionInfo handleRuntimeException(RuntimeException exception,
      Map<String, ExceptionInfo> exceptionInfoMap) {
    Throwable reason = exception.getCause() != null ? exception.getCause() : exception;
    if (exceptionInfoMap.containsKey(reason.getClass().getCanonicalName())) {
      return exceptionInfoMap.get(reason.getClass().getCanonicalName());
    } else {
      return getUnHandledExceptionInfo(reason);
    }
  }

  @Override
  public Response toResponse(Exception exception) {
    if (exception instanceof ServerSideException) {
      log.error(exception.getMessage(), exception);
    } else {
      log.debug(exception.getMessage(), exception);
    }
    ExceptionInfo info = getExceptionInfo(exception, getExceptionInfoMap());
    return Response.status(info.getStatusCode()).entity(
        new HttpErrorBody(info, exception)).build();
  }

}
