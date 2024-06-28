package com.flipkart.fdp.superbi.web.filter;

import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.logger.Auditer;
import com.flipkart.fdp.superbi.core.model.AuditInfo;
import com.flipkart.fdp.superbi.core.model.OperationType;
import com.flipkart.fdp.superbi.cosmos.hystrix.HttpInterceptor;
import com.flipkart.fdp.superbi.web.annotation.Audit;
import com.flipkart.fdp.superbi.web.exception.HttpErrorBody;
import com.flipkart.fdp.superbi.web.exception.SuperBiExceptionMapper;
import com.google.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.FeatureContext;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;

@Slf4j
@Priority(Priorities.USER)
public class RequestAuditFilter implements ContainerRequestFilter, ContainerResponseFilter {

  private final Auditer auditer;

  public static final String USER_HEADER_NAME = "X-AUTHENTICATED-USER";
  public static final String FACTNAME = "factname";

  private long requestStartTime;

  @Context
  private ResourceInfo resourceInfo;

  @Context
  private ResourceContext resourceContext;

  @Context
  private HttpServletRequest request;

  @Context
  private FeatureContext featureContext;

  @Inject
  public RequestAuditFilter(Auditer auditer) {
    this.auditer = auditer;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    requestStartTime = new Date().getTime();
  }

  @SneakyThrows
  private static String urlDecode(String value) {
    return URLDecoder.decode(value, "UTF-8");
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext,
      ContainerResponseContext containerResponseContext) throws IOException {
    try {

      Audit audit = resourceInfo.getResourceClass().getAnnotation(Audit.class);
      if (audit == null) {
        // Do Not Audit
        return;
      }
      long requestEndTime = new Date().getTime();

      String userName = request.getHeader(USER_HEADER_NAME);


      List<String> urlPaths = new ArrayList<>(
          Arrays.asList(request.getRequestURI().trim().split("/")));
      urlPaths.remove(0);
      String errorMessage = "null";
      if (containerResponseContext.getEntity() instanceof HttpErrorBody) {
        HttpErrorBody httpErrorBody = ((HttpErrorBody) containerResponseContext.getEntity());
        if (SuperBiExceptionMapper.INTERNAL_SERVER_ERROR_MESSAGE.equals(
            httpErrorBody.getErrorMessage())) {
          // Get exception trace and audit it
          StringWriter writer = new StringWriter();
          httpErrorBody.getException().printStackTrace(new PrintWriter(writer));
          errorMessage = writer.toString();
        } else {
          errorMessage = httpErrorBody.getErrorMessage();
        }
      }
      AuditInfo.AuditInfoBuilder auditInfoBuilder =
          AuditInfo.builder()
              .createdAt(new Date(requestStartTime)).userName(userName)
              .updatedAt(new Date(requestStartTime))
              .methodName(resourceInfo.getResourceMethod().getName())
              .requestType(containerRequestContext.getMethod()).requestTimeStamp(requestStartTime)
              .httpStatus(containerResponseContext.getStatus())
              .timeTaken(requestEndTime - requestStartTime)
              .host(Optional.of(HttpInterceptor.getHostAddressName()).orElse(null))
              .contextEntity("NA").traceId("NA").jsonPayload("NA")
              .errorMessage(errorMessage).uri(
                  request.getRequestURI().toString() + "?" +  request.getQueryString()
              )
              .entity("Report")
              .action(resourceInfo.getResourceMethod().getName())
              .operationType(OperationType.READ)
              .requestId(MDC.get(RequestIdFilter.REQUEST_ID));

      if(audit.orgAt() >= 0 && audit.orgAt() < urlPaths.size()) {
        auditInfoBuilder.org(urlDecode(urlPaths.get(audit.orgAt())));
      }

      if(audit.namespaceAt() >= 0 && audit.namespaceAt() < urlPaths.size()) {
        auditInfoBuilder.namespace(urlDecode(urlPaths.get(audit.namespaceAt())));
      }

      if(audit.nameAt() >= 0 && audit.nameAt() < urlPaths.size()) {
        auditInfoBuilder.name(urlDecode(urlPaths.get(audit.nameAt())));
      }
      if(containerRequestContext.getProperty(FACTNAME) != null) {
        auditInfoBuilder.factName(containerRequestContext.getProperty(FACTNAME).toString());
      } else {
        auditInfoBuilder.factName("NA");
      }
      auditer.audit(auditInfoBuilder.build());
    } catch (Exception e) {
      log.error("Could not audit {} - {}", resourceInfo.getResourceClass().getCanonicalName(),
          resourceInfo.getResourceMethod().getName(), e);
    }
  }
}