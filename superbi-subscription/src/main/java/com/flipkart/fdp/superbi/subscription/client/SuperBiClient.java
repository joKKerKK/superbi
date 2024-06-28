package com.flipkart.fdp.superbi.subscription.client;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.subscription.configurations.ApiAuthConfig;
import com.flipkart.fdp.superbi.subscription.configurations.SuperbiClientConfig;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.subscription.model.ReportDataResponse;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.net.UrlEscapers;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

@Slf4j
public class SuperBiClient {

  public static final String DOWNLOAD = "download";
  public static final String SUBSCRIPTION = "subscription";
  private final AsyncHttpClient client;
  private final CircuitBreakerRegistry circuitBreakerRegistry;
  private final BulkheadRegistry bulkheadRegistry;
  private final MetricRegistry metricRegistry;
  private final SuperbiClientConfig clientConfig;
  private final Meter successMeter;
  private final Meter failureMeter;
  private final Meter pollMeter;
  private final Meter successResponseMeter;
  private final Meter clientErrorMeter;
  private final Meter serverErrorMeter;

  public SuperBiClient(AsyncHttpClient client,
      CircuitBreakerRegistry circuitBreakerRegistry,
      BulkheadRegistry bulkheadRegistry, MetricRegistry metricRegistry,
      SuperbiClientConfig clientConfig) {
    this.client = client;
    this.circuitBreakerRegistry = circuitBreakerRegistry;
    this.bulkheadRegistry = bulkheadRegistry;
    this.metricRegistry = metricRegistry;
    this.clientConfig = clientConfig;
    this.successMeter = metricRegistry.meter(getMetricsKeyForSuccess());
    this.failureMeter = metricRegistry.meter(getMetricsKeyForFailure());
    pollMeter = metricRegistry.meter(getMetricsKeyForApiResponse("202"));
    successResponseMeter = metricRegistry.meter(getMetricsKeyForApiResponse("200"));
    clientErrorMeter = metricRegistry.meter(getMetricsKeyForApiResponse("4xx"));
    serverErrorMeter = metricRegistry.meter(getMetricsKeyForApiResponse("5xx"));
  }

  @SneakyThrows
  public ReportDataResponse getDataForSubscription(ScheduleInfo scheduleInfo){
    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(SuperBiClient.class.getName());
    Bulkhead bulkhead = bulkheadRegistry.bulkhead(SuperBiClient.class.getName());
    return Bulkhead.decorateSupplier(bulkhead,circuitBreaker.decorateSupplier(()-> getReportData(scheduleInfo))).get();
  }

  @SneakyThrows
  private ReportDataResponse getReportData(ScheduleInfo scheduleInfo){
    Preconditions.checkArgument(StringUtils.isNotBlank(scheduleInfo.getOrg()));
    Preconditions.checkArgument(StringUtils.isNotBlank(scheduleInfo.getNamespace()));
    Preconditions.checkArgument(StringUtils.isNotBlank(scheduleInfo.getReportName()));
    Map<String, String> urlResolveMap = Maps.newHashMap();
    urlResolveMap.put("org", urlEncodePathParam(scheduleInfo.getOrg()));
    urlResolveMap.put("namespace", urlEncodePathParam(scheduleInfo.getNamespace()));
    urlResolveMap.put("reportName", urlEncodePathParam(scheduleInfo.getReportName()));

    StrSubstitutor substitutor = new StrSubstitutor(urlResolveMap);
    String url = substitutor.replace(clientConfig.getDataCallPath());

    Request request = buildRequest(scheduleInfo, url);
    Response response;
    try(Timer.Context context = metricRegistry.timer(getTimerKeyForClient()).time()){

      response = client.executeRequest(
          request).toCompletableFuture().get();
      log.info(MessageFormat.format("Got status code <{0}> from superbi for report-> <{1}> and scheduleId <{2}>"
          ,response.getStatusCode(),scheduleInfo.getReportName(),scheduleInfo.getSubscriptionId()));
      successMeter.mark();
    }catch (Exception e){
      failureMeter.mark();
      log.error("Api call failed for scheduleId <{}> due to {} ",scheduleInfo.getSubscriptionId(),e.getMessage());
      throw e;
    }

    if(response.getStatusCode() == 202){
      pollMeter.mark();
      return JsonUtil.fromJson(response.getResponseBody(),ReportDataResponse.class);
    }
    else if(response.getStatusCode() == 200){
      successResponseMeter.mark();
      return JsonUtil.fromJson(response.getResponseBody(),ReportDataResponse.class);
    }
    else if(response.getStatusCode() >= 400 && response.getStatusCode() <500){
      clientErrorMeter.mark();
      throw new ClientSideException(MessageFormat.format("Superbi exception came with "
              + "statusCode <{0}> and exception <{1}> for scheduleId <{2}>"
          ,response.getStatusCode(), response.getResponseBody(),scheduleInfo.getSubscriptionId()));
    }
    else{
      serverErrorMeter.mark();
      throw new ServerSideException(MessageFormat.format("Superbi exception came with statusCode "
              + "<{0}> exception <{1}>"
              + "for scheduleId <{2}> "
          ,response.getStatusCode(), response.getResponseBody(),scheduleInfo.getSubscriptionId()));
    }

  }


  @SneakyThrows
  private static String urlEncodePathParam(String value) {
    return UrlEscapers.urlPathSegmentEscaper().escape(value);
  }

  private Request buildRequest(ScheduleInfo scheduleInfo,String url) {

    Map<String,List<String>> params = scheduleInfo.getParams();
    ApiAuthConfig apiAuthConfig = scheduleInfo.getIsOTS() ? clientConfig.getApiAuthConfigMap().get(
        DOWNLOAD)
        : clientConfig.getApiAuthConfigMap().get(SUBSCRIPTION);
    return new RequestBuilder("GET").setUrl(getAbsolutePath(url))
        .setQueryParams(params != null && !params.isEmpty() ? params : new HashMap<>())
        .addHeader("X-Client-Id", apiAuthConfig.getClientId())
        .addHeader("X-Client-Secret", apiAuthConfig.getClientSecret())
        .addHeader("x-authenticated-user", scheduleInfo.getOwnerId())
        .addHeader("X-Request-Id",scheduleInfo.getRequestId())
        .build();
  }

  private String getAbsolutePath(String url) {
    return clientConfig.getBasePath() + url;
  }

  private String getMetricsKeyForSuccess() {
    return StringUtils.join(
        Arrays.asList("subscription","data","success"),'.');
  }

  private String getMetricsKeyForFailure() {
    return StringUtils.join(Arrays.asList("subscription","data","failure"),'.');
  }

  private String getTimerKeyForClient() {
    return StringUtils.join(Arrays.asList("subscription","data","timer"),'.');
  }

  private String getMetricsKeyForApiResponse(String statusCode) {
    return StringUtils.join(
        Arrays.asList("subscription","data","status",statusCode),'.');
  }


}
