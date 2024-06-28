package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.FstreamRequest;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.json.JSONArray;
import org.json.JSONException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

public class FStreamClient {

  private static final String AGGREGATE_URI = "/fStream/api/v1/data/aggregate/";
  private String host;
  private String port;

  public FStreamClient(String host, String port) {
    this.host = host;
    this.port = port;
  }

  @SneakyThrows
  public JSONArray getAggregatedData(FstreamRequest fstreamRequest, String fStreamId){
    ObjectMapper objectMapper = new ObjectMapper();
    ResteasyClient client1 = new ResteasyClientBuilder().build();
    ResteasyWebTarget target = client1.target
        (host + ":" + port  + AGGREGATE_URI + fStreamId);
    Response response1 = target.request().post(Entity.json(objectMapper.writeValueAsString(fstreamRequest)));
    String result = response1.readEntity(String.class);
    try {
      return StringUtils.isBlank(result)? new JSONArray("[{}]"): new JSONArray(result);
    } catch (JSONException e) {
      throw new RuntimeException("FStream response object is not proper");
    }
  }

}
