package com.flipkart.fdp.superbi.refresher.dao.fstream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest;
import com.flipkart.resilienthttpclient.exceptions.ServerSideException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.json.JSONArray;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.ServerErrorException;

@Slf4j
public class FStreamClient {
  public static final String SERVICE_NAME = "FSTREAM_SERVICE";
  private final String AGGREGATE_URI = "/fStream/api/v1/data/aggregate/";
  private final String host;
  private final Integer port;
  private final AsyncHttpClient client;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public FStreamClient(AsyncHttpClient asyncHttpClient, String host, Integer port) {
    this.client = asyncHttpClient;
    this.host = host;
    this.port = port;
  }

  @SneakyThrows
  public JSONArray getAggregatedData(FstreamRequest fstreamRequest, String fStreamId) {
    try {
      Request request = buildRequest(fStreamId, fstreamRequest);

      Response response = client
              .executeRequest(request)
              .get();

      if (response == null || !response.hasResponseBody()) {
        throw new InternalServerErrorException("No response from FStream");
      }

      if (response.getStatusCode() >= 400 && response.getStatusCode() < 500) {
          throw new ClientErrorException(response.getResponseBody(), response.getStatusCode());
      }

      if (response.getStatusCode() >= 500 && response.getStatusCode() < 600) {
          throw new ServerErrorException(response.getResponseBody(), response.getStatusCode());
      }

      String result = response.getResponseBody();

      return StringUtils.isBlank(result) ? new JSONArray("[{}]") : new JSONArray(result);
    } catch (ServerSideException | ServerErrorException se) {
          log.error(se.getMessage(), se);
          throw new com.flipkart.fdp.superbi.exceptions.ServerSideException(se);
    } catch (Exception e) {
          throw new com.flipkart.fdp.superbi.exceptions.ClientSideException(e);
    }
  }

  Request buildRequest(String fStreamId, FstreamRequest fstreamRequest) throws JsonProcessingException {
    return new RequestBuilder(HttpMethod.POST).setUrl(host + ":" + port + AGGREGATE_URI + fStreamId)
            .addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON)
            .setBody(objectMapper.writeValueAsString(fstreamRequest))
            .build();
  }
}
