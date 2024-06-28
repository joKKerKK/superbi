package com.flipkart.fdp.superbi.refresher.dao.druid;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidNativeQuery;
import com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidQuery;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.json.JSONArray;


@AllArgsConstructor
@Getter
public class DruidClient {

  public static final String SERVICE_NAME = "DRUID_CLIENT_SERVICE";

  private static final String DATA_URI = "/druid/v2/sql/";
  public static final String ERROR_KEY = "errorClass";
  private final AsyncHttpClient asyncHttpClient;
  private final String host;
  private final Integer port;
  private final List<String> clientSideExceptions;

  @SneakyThrows
  public JSONArray getDataFromQuery(DruidQuery druidQuery) {
    Response response = asyncHttpClient
        .preparePost(host + ":" + port + DATA_URI)
        .setBody(
            JsonUtil.toJson(new DruidNativeQuery(druidQuery.getQuery(), druidQuery.getContext())))
        .setHeader("Content-Type", "application/json")
        .execute()
        .get();

    if (response == null || !response.hasResponseBody()) {
      throw new ServerSideException("No response from Druid");
    }

    if (response.getStatusCode() >= 400 && response.getStatusCode() < 500) {
      throw new ClientSideException(response.getResponseBody());
    }

    if (response.getStatusCode() >= 500 && response.getStatusCode() < 600) {
      Map<String, String> exceptionResponse = JsonUtil
          .fromJson(response.getResponseBody(), Map.class);
      if (exceptionResponse.get(ERROR_KEY) != null && clientSideExceptions
          .contains(exceptionResponse.get(ERROR_KEY).toString())) {
        throw new ClientSideException(response.getResponseBody());
      }
      throw new ServerSideException(response.getResponseBody());
    }

    String result = response.getResponseBody();

    return StringUtils.isBlank(result) ? new JSONArray("[{}]") : new JSONArray(result);

  }
}
