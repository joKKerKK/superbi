package com.flipkart.fdp.superbi.subscription.client;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.subscription.configurations.ApiAuthConfig;
import com.flipkart.fdp.superbi.subscription.configurations.PlatoExecutionClientConfig;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.subscription.model.ScheduleInfo;
import com.flipkart.fdp.superbi.subscription.model.plato.PlatoModelDataResponse;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import javax.ws.rs.HttpMethod;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Optional;

@Slf4j
public class PlatoExecutionClient {
    private final AsyncHttpClient client;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final MetricRegistry metricRegistry;
    private final PlatoExecutionClientConfig clientConfig;
    private final Meter successResponseMeter;
    private final Meter clientErrorMeter;
    private final Meter serverErrorMeter;

    public static final String DOWNLOAD = "download";
    public static final String SUBSCRIPTION = "subscription";

    public PlatoExecutionClient(AsyncHttpClient client,
                                CircuitBreakerRegistry circuitBreakerRegistry,
                                BulkheadRegistry bulkheadRegistry,
                                MetricRegistry metricRegistry,
                                PlatoExecutionClientConfig clientConfig) {
        this.client = client;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.bulkheadRegistry = bulkheadRegistry;
        this.metricRegistry = metricRegistry;
        this.clientConfig = clientConfig;
        successResponseMeter = metricRegistry.meter(getMetricsKeyForApiResponse("200"));
        clientErrorMeter = metricRegistry.meter(getMetricsKeyForApiResponse("4xx"));
        serverErrorMeter = metricRegistry.meter(getMetricsKeyForApiResponse("5xx"));
    }

    @SneakyThrows
    public PlatoModelDataResponse getModelData(ScheduleInfo scheduleInfo, String model) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(SuperBiClient.class.getName());
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(SuperBiClient.class.getName());
        return Bulkhead.decorateSupplier(bulkhead,circuitBreaker.decorateSupplier(()-> fetchModelData(scheduleInfo, model))).get();
    }

    @SneakyThrows
    private PlatoModelDataResponse fetchModelData(ScheduleInfo scheduleInfo, String model) {

        String ownerId = scheduleInfo.getOwnerId();

        ApiAuthConfig apiAuthConfig = scheduleInfo.getIsOTS() ? clientConfig.getApiAuthConfigMap().get(
                DOWNLOAD)
                : clientConfig.getApiAuthConfigMap().get(SUBSCRIPTION);

        log.info("clientConfig.getDataCallPath() : " + clientConfig.getDataCallPath());

        try {
            Request request = buildRequest(HttpMethod.POST, ownerId, Optional.of(model), clientConfig.getDataCallPath(), apiAuthConfig);
            logRequestDetails(request);

            Response response;
            response = client.executeRequest(
                    request).toCompletableFuture().get();
            logResponseDetails(response);

            log.info(MessageFormat.format("Got status code <{0}> from superbi for report-> <{1}> and scheduleId <{2}>"
                    ,response.getStatusCode(),scheduleInfo.getReportName(),scheduleInfo.getSubscriptionId()));

            if(response.getStatusCode() == 200){
                successResponseMeter.mark();
                return JsonUtil.fromJson(response.getResponseBody(), PlatoModelDataResponse.class);
            }
            else if(response.getStatusCode() >= 400 && response.getStatusCode() <500){
                clientErrorMeter.mark();
                throw new ClientSideException(MessageFormat.format("Plato execution exception came with "
                                + "statusCode <{0}> and exception <{1}> for scheduleId <{2}>"
                        ,response.getStatusCode(), response.getResponseBody(),scheduleInfo.getSubscriptionId()));
            }
            else{
                serverErrorMeter.mark();
                throw new ServerSideException(MessageFormat.format("Plato execution exception came with statusCode "
                                + "<{0}> exception <{1}>"
                                + "for scheduleId <{2}> "
                        ,response.getStatusCode(), response.getResponseBody(),scheduleInfo.getSubscriptionId()));
            }
        } catch (Exception e) {
            log.error("Error executing request: ", e);
            throw e;
        }

    }

    private void logRequestDetails(Request request) {
        log.info("Request Details:");
        log.info("URL: {}", request.getUrl());
        log.info("Method: {}", request.getMethod());
        log.info("Headers: {}", request.getHeaders());
        log.info("Body: {}", request.getStringData());
    }

    private void logResponseDetails(Response response) {
        log.info("Response Details:");
        log.info("Status Code: {}", response.getStatusCode());
        log.info("Status Text: {}", response.getStatusText());
        log.info("Headers: {}", response.getHeaders());
        log.info("Body: {}", response.getResponseBody());
    }

    private Request buildRequest(String httpMethod, String user, Optional<String> body, String url, ApiAuthConfig apiAuthConfig) {

        RequestBuilder requestBuilder = new RequestBuilder(httpMethod).setUrl(clientConfig.getBasePath() + url)
                .addHeader("x-authenticated-user", user)
                .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .addHeader("X-Client-Id", apiAuthConfig.getClientId())
                .addHeader("X-Client-Secret", apiAuthConfig.getClientSecret());

        if (body.isPresent()) {
            requestBuilder.setBody(body.get());
        }
        return requestBuilder.build();
    }

    private String getMetricsKeyForApiResponse(String statusCode) {
        return StringUtils.join(
                Arrays.asList("subscription","data","status",statusCode),'.');
    }

}
