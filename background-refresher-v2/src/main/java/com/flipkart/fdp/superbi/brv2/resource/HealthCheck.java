package com.flipkart.fdp.superbi.brv2.resource;

import com.flipkart.fdp.superbi.brv2.services.HealthCheckService;
import com.google.inject.Inject;
import io.swagger.annotations.Api;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by : waghmode.tayappa Date : Jun 20, 2019
 */
@Slf4j
@Api(tags = {"healthCheck"})
@Path(value = "/health")
public class HealthCheck {

  private final HealthCheckService healthCheckService;

  @Inject
  HealthCheck(HealthCheckService service) {
    this.healthCheckService = service;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response elbHealthCheck() {

    HealthCheckService.HealthCheckResponse response = healthCheckService.checkAll();
    return (response.isPassed() ? Response.ok().entity(response).build()
        : Response.status(404).entity(response).build());
  }

}
