package com.flipkart.fdp.superbi.web.resources;

import com.codahale.metrics.annotation.Metered;
import com.flipkart.fdp.superbi.core.api.query.QueryPanel;
import com.flipkart.fdp.superbi.core.model.FetchQueryResponse;
import com.flipkart.fdp.superbi.core.service.DataService;
import com.flipkart.fdp.superbi.web.annotation.Authenticate;
import com.flipkart.fdp.superbi.web.util.MapUtil;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Api(tags = {"Data"})
@Path(value = "/")
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
public class DataResource {

  private final DataService dataService;

  @Inject
  public DataResource(DataService dataService) {
    this.dataService = dataService;
  }

  @POST
  @Metered
  @Path("/executeQueryPanel")
  @ApiOperation(
      value = "executeQueryPanel",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  public FetchQueryResponse executeQueryPanel(QueryPanel queryPanel){
    return dataService.executeQueryPanel(queryPanel);
  }

  @GET
  @Metered
  @Path("/lov/{fact}/{degDim}")
  @ApiOperation(
      value = "lovWithFactAndDimension",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  public FetchQueryResponse getLovWithFactAndDimension(@PathParam("fact") String factName,@PathParam("degDim") String degDim,
      @Context ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    Map<String, String[]> queryParamsMap = MapUtil.convertToMap(queryParams);
    return dataService.getLov(factName,degDim,queryParamsMap);
  }

  @GET
  @Metered
  @Path("/lov/{dimension}/{hierarchy}/{columnName}")
  @ApiOperation(
      value = "lovWithDimensionAndHierarchyAndColumn",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  public FetchQueryResponse getLovWithDimensionAndHierarchyAndColumn(@PathParam("dimension") String dimension,
      @PathParam("hierarchy") String hierarchy,@PathParam("columnName") String columnName,@Context ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    Map<String, String[]> queryParamsMap = MapUtil.convertToMap(queryParams);
    return dataService.getLov(dimension,hierarchy,columnName,queryParamsMap);
  }
}
