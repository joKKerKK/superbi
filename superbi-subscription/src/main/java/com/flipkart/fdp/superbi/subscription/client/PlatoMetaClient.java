package com.flipkart.fdp.superbi.subscription.client;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.fdp.superbi.subscription.configurations.PlatoMetaClientConfig;
import com.flipkart.fdp.superbi.subscription.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.subscription.model.plato.Canvas;
import com.flipkart.fdp.superbi.subscription.model.plato.PlatoMetaApiResponse;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Optional;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import javax.ws.rs.HttpMethod;
import java.text.MessageFormat;
import java.util.Arrays;

public class PlatoMetaClient {
    private final AsyncHttpClient client;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final PlatoMetaClientConfig clientConfig;
    private final Meter successResponseMeter;
    private final Meter clientErrorMeter;
    private final Meter serverErrorMeter;

    public PlatoMetaClient(AsyncHttpClient client,
                           CircuitBreakerRegistry circuitBreakerRegistry,
                           BulkheadRegistry bulkheadRegistry,
                           MetricRegistry metricRegistry,
                           PlatoMetaClientConfig clientConfig) {
        this.client = client;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.bulkheadRegistry = bulkheadRegistry;
        this.clientConfig = clientConfig;
        successResponseMeter = metricRegistry.meter(getMetricsKeyForApiResponse("200"));
        clientErrorMeter = metricRegistry.meter(getMetricsKeyForApiResponse("4xx"));
        serverErrorMeter = metricRegistry.meter(getMetricsKeyForApiResponse("5xx"));
    }

    @SneakyThrows
    public Canvas getCanvas(Long canvasId, String owner) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(SuperBiClient.class.getName());
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(SuperBiClient.class.getName());
        return Bulkhead.decorateSupplier(bulkhead,circuitBreaker.decorateSupplier(()-> fetchCanvas(canvasId, owner))).get();
    }

    @SneakyThrows
    private Canvas fetchCanvas(Long canvasId, String owner) {
        String url = MessageFormat.format(clientConfig.getCanvasApiPath(), canvasId.toString());

        Request request = buildRequest(url, owner);

        Response response = client.executeRequest(
                request).toCompletableFuture().get();

        if(response.getStatusCode() == 200){
            successResponseMeter.mark();
            PlatoMetaApiResponse<Canvas> platoMetaApiResponse = JsonUtil.fromJson(response.getResponseBody(), new TypeReference<PlatoMetaApiResponse<Canvas>>() {});
            return platoMetaApiResponse.getData().get();
        }
        else if(response.getStatusCode() >= 400 && response.getStatusCode() <500){
            clientErrorMeter.mark();
            throw new ClientSideException(MessageFormat.format("Plato get canvas exception came with "
                            + "statusCode <{0}> and exception <{1}>"
                    ,response.getStatusCode(), response.getResponseBody()));
        }
        else{
            serverErrorMeter.mark();
            throw new ClientSideException(MessageFormat.format("Plato get canvas exception came with "
                            + "statusCode <{0}> and exception <{1}>"
                    ,response.getStatusCode(), response.getResponseBody()));
        }
    }

    @SneakyThrows
    public Optional<Canvas.Tab.Widget> getWidget(Long widgetId, String owner) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(SuperBiClient.class.getName());
        Bulkhead bulkhead = bulkheadRegistry.bulkhead(SuperBiClient.class.getName());
        return Bulkhead.decorateSupplier(bulkhead,circuitBreaker.decorateSupplier(()-> fetchWidget(widgetId, owner))).get();
    }

    @SneakyThrows
    private com.google.common.base.Optional<Canvas.Tab.Widget> fetchWidget(Long widgetId, String owner) {
        String url = MessageFormat.format(clientConfig.getWidgetApiPath(), 0, widgetId.toString(), true);

        Request request = buildRequest(url, owner);

        Response response = client.executeRequest(
                request).toCompletableFuture().get();

        if(response.getStatusCode() == 200){
            successResponseMeter.mark();
            PlatoMetaApiResponse<Canvas.Tab.Widget> platoMetaApiResponse = JsonUtil.fromJson(response.getResponseBody(), new TypeReference<PlatoMetaApiResponse<Canvas.Tab.Widget>>() {});
            return platoMetaApiResponse.getData();
        }
        else if(response.getStatusCode() >= 400 && response.getStatusCode() <500){
            clientErrorMeter.mark();
            throw new ClientSideException(MessageFormat.format("Plato get widgets exception came with "
                            + "statusCode <{0}> and exception <{1}>"
                    ,response.getStatusCode(), response.getResponseBody()));
        }
        else{
            serverErrorMeter.mark();
            throw new ClientSideException(MessageFormat.format("Plato get widgets exception came with "
                            + "statusCode <{0}> and exception <{1}>"
                    ,response.getStatusCode(), response.getResponseBody()));
        }
    }

    private Request buildRequest(String url, String user) {
        return new RequestBuilder(HttpMethod.GET).setUrl(clientConfig.getBasePath() + url)
                .addHeader("x-authenticated-user", user)
                .build();
    }

    private String getMetricsKeyForApiResponse(String statusCode) {
        return StringUtils.join(
                Arrays.asList("subscription","data","status",statusCode),'.');
    }
}
