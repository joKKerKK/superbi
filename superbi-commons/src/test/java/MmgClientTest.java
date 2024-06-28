import static org.mockito.Matchers.any;

import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClient;
import com.flipkart.fdp.superbi.http.client.mmg.MmgClientConfiguration;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class MmgClientTest {

  private MmgClient mmgClient;
  private AsyncHttpClient httpClient;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    httpClient = PowerMockito.mock(AsyncHttpClient.class);
    mmgClient = new MmgClient(httpClient,
        new MmgClientConfiguration("http://localhost", 90, 0, 0, 0, 0, 0, 0, 0, null));
  }

  @Test
  public void testMmgResponseWhenResponseStatusIs200()
      throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    CompletableFuture<Response> completableFuture = PowerMockito.mock(CompletableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(response.hasResponseBody()).thenReturn(true);
    PowerMockito.when(response.getResponseBody()).thenReturn("12345");
    PowerMockito.when(response.getStatusCode()).thenReturn(200);
    PowerMockito.when(httpClient.executeRequest(any(Request.class))).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.toCompletableFuture()).thenReturn(completableFuture);
    PowerMockito.when(completableFuture.get()).thenReturn(response);

    Long refreshTime = mmgClient
        .getFactRefreshTime("fact", "storeIdentifier");

    Assert.assertEquals(Long.valueOf("12345"), refreshTime);

  }

  @Test
  public void testMmgResponseWhenResponseStatusIs204()
      throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    CompletableFuture<Response> completableFuture = PowerMockito.mock(CompletableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(httpClient.executeRequest(any(Request.class))).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.toCompletableFuture()).thenReturn(completableFuture);
    PowerMockito.when(completableFuture.get()).thenReturn(response);
    PowerMockito.when(response.getStatusCode()).thenReturn(204);

    Long refreshTime = mmgClient
        .getFactRefreshTime("fact", "storeIdentifier");

    Assert.assertEquals(Long.valueOf(0), refreshTime);

  }

  @Test
  public void testMmgResponseWhenResponseStatusIs404()
      throws ExecutionException, InterruptedException {
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    CompletableFuture<Response> completableFuture = PowerMockito.mock(CompletableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(httpClient.executeRequest(any(Request.class))).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.toCompletableFuture()).thenReturn(completableFuture);
    PowerMockito.when(completableFuture.get()).thenReturn(response);
    PowerMockito.when(response.getStatusCode()).thenReturn(404);

    Long refreshTime = mmgClient
        .getFactRefreshTime("fact", "storeIdentifier");

    Assert.assertEquals(Long.valueOf(0), refreshTime);
  }

  @Test
  public void testMmgResponseWhenResponseStatusIs500()
      throws ExecutionException, InterruptedException {
    thrown.expect(ServerSideException.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    CompletableFuture<Response> completableFuture = PowerMockito.mock(CompletableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(httpClient.executeRequest(any(Request.class))).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.toCompletableFuture()).thenReturn(completableFuture);
    PowerMockito.when(completableFuture.get()).thenReturn(response);
    PowerMockito.when(response.getStatusCode()).thenReturn(500);
    PowerMockito
        .when(listenableFuture.get())
        .thenReturn(response);

    mmgClient.getFactRefreshTime("fact", "storeIdentifier");
  }

  @Test
  public void testMmgResponseWhenResponseStatusWithOtherStatusCode()
      throws ExecutionException, InterruptedException {
    thrown.expect(ServerSideException.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    CompletableFuture<Response> completableFuture = PowerMockito.mock(CompletableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(httpClient.executeRequest(any(Request.class))).thenReturn(listenableFuture);
    PowerMockito.when(listenableFuture.toCompletableFuture()).thenReturn(completableFuture);
    PowerMockito.when(completableFuture.get()).thenReturn(response);
    PowerMockito.when(response.hasResponseBody()).thenReturn(true);
    PowerMockito.when(response.getStatusCode()).thenReturn(403);
    PowerMockito
        .when(listenableFuture.get())
        .thenReturn(response);

    mmgClient.getFactRefreshTime("fact", "storeIdentifier");
  }

  @Test
  public void testMmgResponseWhenResponseStatusWithAnyException()
      throws ExecutionException, InterruptedException {
    thrown.expect(ServerSideException.class);
    ListenableFuture<Response> listenableFuture = PowerMockito.mock(ListenableFuture.class);
    CompletableFuture<Response> completableFuture = PowerMockito.mock(CompletableFuture.class);
    Response response = PowerMockito.mock(Response.class);
    PowerMockito.when(httpClient.executeRequest(any(Request.class))).thenThrow(new RuntimeException());

    mmgClient.getFactRefreshTime("fact", "storeIdentifier");
  }
}
