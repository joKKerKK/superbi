package com.flipkart.fdp.superbi.web.filter;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * Created by akshaya.sharma on 27/08/19
 * This is an extension of io.dropwizard.jersey.filter.RequestIdFilter so that we can user
 * X-Request-Id in logs by using MDC of Slf4j
 */
@Slf4j
public class RequestIdFilter implements Filter {
  public static final String REQUEST_ID = "X-Request-Id";

  private static UUID generateRandomUuid() {
    Random rnd = ThreadLocalRandom.current();
    long mostSig = rnd.nextLong();
    long leastSig = rnd.nextLong();
    mostSig &= -61441L;
    mostSig |= 16384L;
    leastSig &= 4611686018427387903L;
    leastSig |= -9223372036854775808L;
    return new UUID(mostSig, leastSig);
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    HttpServletResponse httpServletResponse = (HttpServletResponse) response;
    String id = httpServletRequest.getHeader(REQUEST_ID);
    if (Strings.isNullOrEmpty(id)) {
      id = generateRandomUuid().toString();
    }

    MDC.put(REQUEST_ID, id);

    /**
     * Access Logs in dropwizard can not access MDC(limitation of dropwizard's logback-access
     * So setting it in requestAttributes too.
     */
    request.setAttribute(REQUEST_ID, id);

    ((HttpServletResponse) response).setHeader(REQUEST_ID, id);
    filterChain.doFilter(request, response);
  }

  @Override
  public void destroy() {

  }
}
