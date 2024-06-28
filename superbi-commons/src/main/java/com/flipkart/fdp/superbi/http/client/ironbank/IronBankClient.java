package com.flipkart.fdp.superbi.http.client.ironbank;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.net.UrlEscapers;
import com.google.gson.Gson;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

public class IronBankClient {

  public static final int OK_STATUS = 200;
  private final AsyncHttpClient client;
  public static final String SERVICE_NAME = "IRON_BANK_SERVICE";
  private static final Gson gson = new Gson();

  private static final String GET_QUEUE_PATH =
      "/queue/${billingOrg}";


  private final IronBankConfiguration ironBankConfiguration;

  public IronBankClient(AsyncHttpClient client,IronBankConfiguration ironBankConfiguration) {
    this.ironBankConfiguration = ironBankConfiguration;
    this.client = client;

  }

  private String getAbsolutePath(String url) {
    return StringUtils.join(ironBankConfiguration.getBasePath(),":",
        ironBankConfiguration.getPort(),url);
  }

  @SneakyThrows
  private static String urlEncodePathParam(String value) {
    return UrlEscapers.urlPathSegmentEscaper().escape(value);
  }

  @SneakyThrows
  public Map<String,Object> getQueue(String billingOrg) {
    Preconditions.checkArgument(StringUtils.isNotBlank(billingOrg));
    Map<String, String> urlResolveMap = Maps.newHashMap();
    urlResolveMap.put("billingOrg", urlEncodePathParam(billingOrg));

    StrSubstitutor substitutor = new StrSubstitutor(urlResolveMap);
    String url = substitutor.replace(GET_QUEUE_PATH);
    Request request = new RequestBuilder("GET").setUrl(getAbsolutePath(url)).build();

    org.asynchttpclient.Response response1 = client.executeRequest(
        request).toCompletableFuture().get();

    if(response1.getStatusCode() != OK_STATUS) {
      throw new RuntimeException("Could not get security attributes for user");
    }

    String responseBody = response1.getResponseBody();
    Map<String,Object> ironBankResponse = gson.fromJson(responseBody, Map.class);

    return ironBankResponse;
  }

}
