package com.flipkart.fdp.superbi.http.client.mmg;

import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.net.UrlEscapers;
import java.text.MessageFormat;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

@Slf4j
public class MmgClient {

  public static final int OK_STATUS = 200;
  public static final int NOT_FOUND_STATUS = 404;
  public static final int NO_CONTENT_STATUS = 204;
  private final AsyncHttpClient client;
  public static final String SERVICE_NAME = "MMG_SERVICE";

  private static final String GET_LAST_REFRESH = "/datasources/${factName}/getLastRefresh?storeIdentifier=${storeIdentifier}";


  private final MmgClientConfiguration mmgClientConfiguration;

  public MmgClient(AsyncHttpClient client, MmgClientConfiguration mmgClientConfiguration) {
    this.mmgClientConfiguration = mmgClientConfiguration;
    this.client = client;

  }

  private String getAbsolutePath(String url) {
    return StringUtils.join(mmgClientConfiguration.getBasePath(), ":",
        mmgClientConfiguration.getPort(), url);
  }

  @SneakyThrows
  private static String urlEncodePathParam(String value) {
    return UrlEscapers.urlPathSegmentEscaper().escape(value);
  }

  @SneakyThrows
  public Long getFactRefreshTime(String factName, String storeIdentifier) {
    Preconditions.checkArgument(StringUtils.isNotBlank(factName));
    Map<String, String> urlResolveMap = Maps.newHashMap();
    urlResolveMap.put("factName", urlEncodePathParam(factName));
    urlResolveMap.put("storeIdentifier", storeIdentifier );

    StrSubstitutor substitutor = new StrSubstitutor(urlResolveMap);
    String url = substitutor.replace(GET_LAST_REFRESH);
    log.info("url : {}", url);
    Request request = new RequestBuilder("GET").setUrl(getAbsolutePath(url)).build();

    try {
      org.asynchttpclient.Response response = client.executeRequest(
          request).toCompletableFuture().get();
      if (response.getStatusCode() == OK_STATUS) {
        String responseBody = response.getResponseBody();
        log.info( "responseBody : {}", responseBody);
        return Long.valueOf(responseBody);
      }

      if (response.getStatusCode() == NOT_FOUND_STATUS
          || response.getStatusCode() == NO_CONTENT_STATUS) {
        return 0L;
      }
      throw new ServerSideException(MessageFormat.format(
          "Could not get last refresh time for {0} due to improper response and status code {1}",
          factName,
          response.getStatusCode()));
    } catch (Exception e) {
      throw new ServerSideException(
          MessageFormat.format("Could not get last refresh time for {0} due to {1} ", factName,
              e.getMessage()));
    }
  }

}
