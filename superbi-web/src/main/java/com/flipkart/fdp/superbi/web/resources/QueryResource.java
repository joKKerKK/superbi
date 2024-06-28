package com.flipkart.fdp.superbi.web.resources;

import com.codahale.metrics.annotation.Metered;
import com.flipkart.fdp.superbi.core.model.FetchQueryResponse;
import com.flipkart.fdp.superbi.core.model.QueryRefreshRequest;
import com.flipkart.fdp.superbi.core.service.DataService;
import com.flipkart.fdp.superbi.web.annotation.Audit;
import com.flipkart.fdp.superbi.web.annotation.Authenticate;
import com.google.inject.Inject;
import io.swagger.annotations.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Slf4j
@Api(tags = {"Query"})
@Path(value = "/query/{org}/{namespace}")
@Audit(orgAt = 1, namespaceAt = 2, nameAt = -1)
@Produces(MediaType.APPLICATION_JSON)
@SwaggerDefinition(
    securityDefinition = @SecurityDefinition(
        apiKeyAuthDefinitions= {
            @ApiKeyAuthDefinition(
                in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
                name = "X-Client-Id",
                key = "X-Client-Id"
            ),
            @ApiKeyAuthDefinition(
                in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
                name = "X-Client-Secret",
                key = "X-Client-Secret"
            ),
            @ApiKeyAuthDefinition(
                in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
                name = "x-authenticated-user",
                key = "x-authenticated-user"
            )
        }
    )
)
@Authenticate
public class QueryResource {
  private final DataService dataService;

  @Inject
  public QueryResource(DataService dataService) {
    this.dataService = dataService;
  }

  @POST
  @Path("/execute")
  @ApiOperation(
      value = "executeQuery",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  @Metered
  public FetchQueryResponse execute(QueryRefreshRequest queryRefreshRequest, @Context ContainerRequestContext requestContext) {
    return dataService.executeQuery(queryRefreshRequest);
  }

}