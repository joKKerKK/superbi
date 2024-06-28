package com.flipkart.fdp.superbi.refresher.dao.fstream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

@RunWith(PowerMockRunner.class)
public class FStreamClientTest {
    private FStreamClient fStreamClient;
    private AsyncHttpClient httpClient;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        httpClient = PowerMockito.mock(AsyncHttpClient.class);
        fStreamClient = PowerMockito.spy(new FStreamClient(httpClient, "host", 80));
    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs200() throws IOException, ExecutionException, InterruptedException {
        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(response.hasResponseBody()).thenReturn(true);
        PowerMockito.when(response.getResponseBody()).thenReturn("[{\"id\":1234}]");

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);
        PowerMockito.when(listenableFuture.get()).thenReturn(response);

        JSONArray actualResponse = fStreamClient.getAggregatedData(fstreamRequest, "1234");

        Assert.assertEquals("[{\"id\":1234}]", actualResponse.toString());

    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs200ButResponseBodyEmpty() throws IOException, ExecutionException, InterruptedException {
        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(response.hasResponseBody()).thenReturn(true);
        PowerMockito.when(response.getResponseBody()).thenReturn("");

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);
        PowerMockito.when(listenableFuture.get()).thenReturn(response);

        JSONArray actualResponse = fStreamClient.getAggregatedData(fstreamRequest, "1234");

        Assert.assertEquals("[{}]", actualResponse.toString());

    }

    @Test
    public void testfStreamResponseWhenResponseIsNULL() throws ExecutionException, InterruptedException, JsonProcessingException {
        thrown.expect(com.flipkart.fdp.superbi.exceptions.ServerSideException.class);
        thrown.expectMessage("No response from FStream");
        thrown.reportMissingExceptionWithMessage("Exception expected");

        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);
        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);


        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);
        PowerMockito
                .when(listenableFuture.get())
                .thenReturn(null);

        fStreamClient.getAggregatedData(fstreamRequest, "1234");
    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs500() throws ExecutionException, InterruptedException, JsonProcessingException {
        thrown.expect(com.flipkart.fdp.superbi.exceptions.ServerSideException.class);
        thrown.expectMessage("{\"code\": 500, \"message\": \"Something went wrong\"}");
        thrown.reportMissingExceptionWithMessage("Exception expected");

        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);
        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);

        PowerMockito
                .when(listenableFuture.get())
                .thenThrow(new InternalServerErrorException("{\"code\": 500, \"message\": \"Something went wrong\"}"));

        fStreamClient.getAggregatedData(new FstreamRequest(), "1234");
    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs400() throws ExecutionException, InterruptedException, JsonProcessingException {
        thrown.expect(com.flipkart.fdp.superbi.exceptions.ClientSideException.class);
        thrown.expectMessage("{\"code\": 400, \"message\": \"Bad request\"}");
        thrown.reportMissingExceptionWithMessage("Exception expected");

        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);
        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);

        PowerMockito
                .when(listenableFuture.get())
                .thenThrow(new BadRequestException("{\"code\": 400, \"message\": \"Bad request\"}"));

        fStreamClient.getAggregatedData(new FstreamRequest(), "1234");
    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs404() throws ExecutionException, InterruptedException, JsonProcessingException {
        thrown.expect(com.flipkart.fdp.superbi.exceptions.ClientSideException.class);
        thrown.expectMessage("{\"code\": 404, \"message\": \"Not found\"}");
        thrown.reportMissingExceptionWithMessage("Exception expected");

        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);
        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);

        PowerMockito
                .when(listenableFuture.get())
                .thenThrow(new NotFoundException("{\"code\": 404, \"message\": \"Not found\"}"));

        fStreamClient.getAggregatedData(new FstreamRequest(), "1234");
    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs400ButNoErrorFromFStream() throws ExecutionException, InterruptedException, JsonProcessingException {
        thrown.expect(com.flipkart.fdp.superbi.exceptions.ClientSideException.class);
        thrown.expectMessage("Bad Request");
        thrown.reportMissingExceptionWithMessage("Exception expected");

        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
        Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(response.hasResponseBody()).thenReturn(true);
        PowerMockito.when(response.getResponseBody()).thenReturn("Bad Request");
        PowerMockito.when(response.getStatusCode()).thenReturn(400);
        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);
        PowerMockito.when(listenableFuture.get()).thenReturn(response);

        fStreamClient.getAggregatedData(new FstreamRequest(), "1234");
    }

    @Test
    public void testfStreamResponseWhenResponseStatusIs500ButNoErrorFromFStream() throws ExecutionException, InterruptedException, JsonProcessingException {
        thrown.expect(com.flipkart.fdp.superbi.exceptions.ServerSideException.class);
        thrown.expectMessage("Internal server error");
        thrown.reportMissingExceptionWithMessage("Exception expected");

        ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
        Response response = PowerMockito.mock(Response.class);
        PowerMockito.when(response.hasResponseBody()).thenReturn(true);
        PowerMockito.when(response.getResponseBody()).thenReturn("Internal server error");
        PowerMockito.when(response.getStatusCode()).thenReturn(500);
        FstreamRequest fstreamRequest = new FstreamRequest();
        Request request = PowerMockito.mock(Request.class);

        PowerMockito.when(httpClient.executeRequest(request)).thenReturn(listenableFuture);
        PowerMockito.doReturn(request).when(fStreamClient).buildRequest("1234", fstreamRequest);

        PowerMockito.when(listenableFuture.get()).thenReturn(response);

        fStreamClient.getAggregatedData(new FstreamRequest(), "1234");
    }
}
