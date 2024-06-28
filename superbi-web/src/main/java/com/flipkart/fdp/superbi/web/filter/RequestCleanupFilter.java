package com.flipkart.fdp.superbi.web.filter;

import com.flipkart.fdp.dao.common.util.EntityManagerProvider;
import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

/**
 * Created by akshaya.sharma on 08/07/19
 */

public class RequestCleanupFilter implements ContainerResponseFilter, ContainerRequestFilter {
  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    EntityManagerProvider.clear();
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext,
      ContainerResponseContext containerResponseContext) throws IOException {
    EntityManagerProvider.clear();
  }
}