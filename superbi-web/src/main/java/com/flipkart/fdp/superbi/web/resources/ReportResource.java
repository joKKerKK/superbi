package com.flipkart.fdp.superbi.web.resources;

import com.codahale.metrics.annotation.Metered;
import com.flipkart.fdp.superbi.core.api.ReportSTO;
import com.flipkart.fdp.superbi.core.model.CacheDetail;
import com.flipkart.fdp.superbi.core.model.DownloadResponse;
import com.flipkart.fdp.superbi.core.model.FetchQueryResponse;
import com.flipkart.fdp.superbi.core.model.TargetDataResponse;
import com.flipkart.fdp.superbi.core.service.ReportService;
import com.flipkart.fdp.superbi.core.service.TargetDataService;
import com.flipkart.fdp.superbi.web.annotation.Audit;
import com.flipkart.fdp.superbi.web.annotation.Authenticate;
import com.flipkart.fdp.superbi.web.util.MapUtil;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Map;

/**
 * Created by akshaya.sharma on 08/07/19
 */
@Slf4j
@Api(tags = {"Report"})
@Path(value = "/reports/{org}/{namespace}/{name}")
@Audit(orgAt = 1, namespaceAt = 2, nameAt = 3)
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
public class ReportResource {
  private final ReportService reportService;
  private final TargetDataService targetDataService;

  @Inject
  public ReportResource(ReportService reportService,
      TargetDataService targetDataService) {
    this.reportService = reportService;
    this.targetDataService = targetDataService;
  }

  @GET
  @Path("/")
  public ReportSTO getReport(@PathParam("org") String org, @PathParam("namespace") String namespace,
      @PathParam("name") String reportName) {
    throw new NotImplementedException("URL not implemented yet");
  }

  @GET
  @Path("/explain")
  public FetchQueryResponse getReportNativeQuery(@PathParam("org") String org, @PathParam("namespace") String namespace,
      @PathParam("name") String reportName,@Context ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    Map<String, String[]> params1 = MapUtil.convertToMap(queryParams);
    return reportService.getNativeQuery(org, namespace, reportName, params1);
  }

  @GET
  @Path("/data")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "params", value = "string", required = false,
          dataType = "string",
          paramType = "query")})
  @ApiOperation(
      value = "getReportData",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  @Metered
  public FetchQueryResponse getReportData(@PathParam("org") String org,
      @PathParam("namespace") String namespace,
      @PathParam("name") String reportName, @Context ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    Map<String, String[]> params1 = MapUtil.convertToMap(queryParams);
    return reportService.getReportData(org, namespace, reportName, params1, requestContext);
  }

  @GET
  @Path("/dataLocation")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "params", value = "string", required = false,
          dataType = "string",
          paramType = "query")})
  @ApiOperation(
      value = "getReportDataLocation",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  @Metered
  public CacheDetail getReportDataLocation(@PathParam("org") String org,
      @PathParam("namespace") String namespace,
      @PathParam("name") String reportName, @Context ContainerRequestContext requestContext) {
    throw new NotImplementedException("URL not implemented yet");
  }

  @GET
  @Path("/targetData")
  @ApiImplicitParams({
      @ApiImplicitParam(name = "params", value = "string", required = false,
          dataType = "string",
          paramType = "query")})
  @ApiOperation(
      value = "getReportData",
      authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  @Metered
  public TargetDataResponse getTargetData(@PathParam("org") String org,
      @PathParam("namespace") String namespace,
      @PathParam("name") String reportName, @Context ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();

    Map<String, String[]> params1 = MapUtil.convertToMap(queryParams);
    return targetDataService.getTargetData(org, namespace, reportName, params1);
  }

  @GET
  @Path("/download")
  @ApiImplicitParams({
          @ApiImplicitParam(name = "params", value = "string", dataType = "string", paramType = "query")})
  @ApiOperation(
          value = "download",
          authorizations={@Authorization("X-Client-Id"), @Authorization("X-Client-Secret"), @Authorization("x-authenticated-user")}
  )
  @Metered
  public DownloadResponse downloadReport(@PathParam("org") String org, @PathParam("namespace") String namespace,
                                                       @PathParam("name") String reportName, @Context ContainerRequestContext requestContext) {
    MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
    Map<String, String[]> paramsMap = MapUtil.convertToMap(queryParams);
    return reportService.downloadReport(org, namespace, reportName, paramsMap);
  }
}
