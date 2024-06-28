package com.flipkart.fdp.superbi.refresher.dao.druid;

import static org.mockito.Matchers.any;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidQuery;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class DruidClientTest {

  private DruidClient druidClient;
  private AsyncHttpClient httpClient;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    httpClient = PowerMockito.mock(AsyncHttpClient.class);
    druidClient = new DruidClient(httpClient, "host", 80, Arrays.asList("org.apache.calcite.sql.parser.SqlParseException"));
  }

  @Test
  public void testDruidResponseWhenResponseStatusIs200() throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    BoundRequestBuilder boundRequestBuilder = PowerMockito.mock(BoundRequestBuilder.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(response.hasResponseBody()).thenReturn(true);
    PowerMockito.when(response.getResponseBody()).thenReturn("[{\"id\":1234}]");
    PowerMockito.when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setHeader(Mockito.any(),Mockito.anyString())).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.get()).thenReturn(response);

    JSONArray actualResponse = druidClient.getDataFromQuery(new DruidQuery("query",new ArrayList<>(),new HashMap<>()));

    Assert.assertEquals("[{\"id\":1234}]", actualResponse.toString());

  }

  @Test
  public void testDruidResponseWhenResponseStatusIs200ButResponseBodyEmpty() throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    BoundRequestBuilder boundRequestBuilder = PowerMockito.mock(BoundRequestBuilder.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(response.hasResponseBody()).thenReturn(true);
    PowerMockito.when(response.getResponseBody()).thenReturn("");
    PowerMockito.when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setHeader(Mockito.any(),Mockito.anyString())).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.get()).thenReturn(response);

    JSONArray actualResponse = druidClient.getDataFromQuery(new DruidQuery("query",new ArrayList<>(),new HashMap<>()));

    Assert.assertEquals("[{}]", actualResponse.toString());

  }

  @Test
  public void testDruidResponseWhenResponseIsNULL() throws ExecutionException, InterruptedException {
    thrown.expect(ServerSideException.class);
    thrown.expectMessage("No response from Druid");
    thrown.reportMissingExceptionWithMessage("Exception expected");

    BoundRequestBuilder boundRequestBuilder = PowerMockito.mock(BoundRequestBuilder.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    PowerMockito.when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setHeader(Mockito.any(),Mockito.anyString())).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.execute()).thenReturn(listenableFuture);

    PowerMockito
        .when(listenableFuture.get())
        .thenReturn(null);

    druidClient.getDataFromQuery(new DruidQuery("query",new ArrayList<>(),new HashMap<>()));
  }

  @Test
  public void testDruidResponseWhenResponseStatusIs500WithExceptionInList() throws ExecutionException, InterruptedException {
    thrown.expect(ClientSideException.class);
    thrown.expectMessage("{\"error\":\"Unknown exception\",\"errorMessage\":\"org.apache.calcite.runtime.CalciteContextException: From line 1, column 107 to line 1, column 134: No match found for function signature TsIMESTAMP_TO_MILLIS(<TIMESTAMP>)\",\"errorClass\":\"org.apache.calcite.sql.parser.SqlParseException\",\"host\":null}");
    thrown.reportMissingExceptionWithMessage("Exception expected");
    Response response = PowerMockito.mock(Response.class);
    BoundRequestBuilder boundRequestBuilder = PowerMockito.mock(BoundRequestBuilder.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    PowerMockito.when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setHeader(Mockito.any(),Mockito.anyString())).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
    PowerMockito.when(response.hasResponseBody()).thenReturn(true);
    PowerMockito.when(response.getStatusCode()).thenReturn(500);
    PowerMockito.when(response.getResponseBody()).thenReturn("{\"error\":\"Unknown exception\",\"errorMessage\":\"org.apache.calcite.runtime.CalciteContextException: From line 1, column 107 to line 1, column 134: No match found for function signature TsIMESTAMP_TO_MILLIS(<TIMESTAMP>)\",\"errorClass\":\"org.apache.calcite.sql.parser.SqlParseException\",\"host\":null}");
    PowerMockito
        .when(listenableFuture.get())
        .thenReturn(response);

    druidClient.getDataFromQuery(new DruidQuery("query",new ArrayList<>(),new HashMap<>()));
  }

  @Test
  public void testDruidResponseWhenResponseStatusIs500WithoutExceptionInList() throws ExecutionException, InterruptedException {
    thrown.expect(ServerSideException.class);
    thrown.expectMessage("{\"error\":\"Unknown exception\",\"errorMessage\":\"org.apache.calcite.runtime.CalciteContextException: From line 1, column 107 to line 1, column 134: No match found for function signature TsIMESTAMP_TO_MILLIS(<TIMESTAMP>)\",\"errorClass\":\"org.apache.calcite.tools.ValidationException\",\"host\":null}");
    thrown.reportMissingExceptionWithMessage("Exception expected");
    Response response = PowerMockito.mock(Response.class);
    BoundRequestBuilder boundRequestBuilder = PowerMockito.mock(BoundRequestBuilder.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    PowerMockito.when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setHeader(Mockito.any(),Mockito.anyString())).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
    PowerMockito.when(response.hasResponseBody()).thenReturn(true);
    PowerMockito.when(response.getStatusCode()).thenReturn(500);
    PowerMockito.when(response.getResponseBody()).thenReturn("{\"error\":\"Unknown exception\",\"errorMessage\":\"org.apache.calcite.runtime.CalciteContextException: From line 1, column 107 to line 1, column 134: No match found for function signature TsIMESTAMP_TO_MILLIS(<TIMESTAMP>)\",\"errorClass\":\"org.apache.calcite.tools.ValidationException\",\"host\":null}");
    PowerMockito
        .when(listenableFuture.get())
        .thenReturn(response);

    druidClient.getDataFromQuery(new DruidQuery("query",new ArrayList<>(),new HashMap<>()));
  }

  @Test
  public void testDruidResponseWhenResponseStatusIs400() throws ExecutionException, InterruptedException {
    thrown.expect(ClientSideException.class);
    thrown.expectMessage("{\"code\": 400, \"message\": \"Bad request\"}");
    thrown.reportMissingExceptionWithMessage("Exception expected");

    BoundRequestBuilder boundRequestBuilder = PowerMockito.mock(BoundRequestBuilder.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    PowerMockito.when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.setHeader(Mockito.any(),Mockito.anyString())).thenReturn(boundRequestBuilder);
    PowerMockito.when(boundRequestBuilder.execute()).thenReturn(listenableFuture);

    PowerMockito
        .when(listenableFuture.get())
        .thenThrow(new ClientSideException("{\"code\": 400, \"message\": \"Bad request\"}"));

    druidClient.getDataFromQuery(new DruidQuery("query",new ArrayList<>(),new HashMap<>()));
  }

}
