package com.flipkart.fdp.superbi.web.filter;

import com.flipkart.fdp.auth.common.exception.NotAuthorizedException;
import com.flipkart.fdp.superbi.core.config.ApiKey;
import com.flipkart.fdp.superbi.core.config.ClientPrivilege;
import com.flipkart.fdp.superbi.core.context.ContextProvider;
import com.flipkart.fdp.superbi.web.annotation.Authenticate;
import com.flipkart.fdp.superbi.web.configurations.ApplicationConfig;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 19/07/19
 */
@Priority(Priorities.AUTHENTICATION)
@Provider
public class AuthenticationFilter implements ContainerRequestFilter {

  public static final String CLIENT_ID_HEADER_NAME = "X-Client-Id";
  public static final String CLIENT_SECRET_HEADER_NAME = "X-CLIENT-SECRET";
  public static final String USER_HEADER_NAME = "X-AUTHENTICATED-USER";

  public static final String SYSTEM_USER_HEADER_NAME = "X-SYSTEM-USER";

  @Context
  private HttpServletRequest request;

  @Context
  private ResourceInfo resourceInfo;

  private final ApplicationConfig applicationConfig;

  @Inject
  public AuthenticationFilter(ApplicationConfig applicationConfig) {
    this.applicationConfig = applicationConfig;
  }

  private boolean isValidCredentials(String clientId, String clientSecret) {
    ApiKey apiKey = new ApiKey(clientId, clientSecret);
    return applicationConfig.getApiKeys().contains(apiKey);
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    Authenticate authenticate = Optional.ofNullable(resourceInfo.getResourceMethod().getAnnotation(Authenticate.class))
            .orElse(resourceInfo.getResourceClass().getAnnotation(Authenticate.class));

    if (authenticate == null) {
      // Skip this, as resource is not protected
      return;
    }
    String clientId = request.getHeader(CLIENT_ID_HEADER_NAME);
    String clientSecret = request.getHeader(CLIENT_SECRET_HEADER_NAME);

    if (!isValidCredentials(clientId, clientSecret)) {
      throw new NotAuthorizedException(
          "Invalid " + AuthenticationFilter.CLIENT_ID_HEADER_NAME + " or " + AuthenticationFilter
              .CLIENT_SECRET_HEADER_NAME);
    }

    ClientPrivilege clientPrivilege = applicationConfig.getClientPrivilege(clientId);
    String userName = request.getHeader(USER_HEADER_NAME);
    String systemUser = request.getHeader(SYSTEM_USER_HEADER_NAME);
    if (authenticate.isUserRequired() && StringUtils.isBlank(userName)) {
      throw new NotAuthorizedException("Invalid " + USER_HEADER_NAME);
    }
    ContextProvider.setCurrentSuperBiContext(clientId, userName, Optional.ofNullable(systemUser), clientPrivilege);
  }
}
