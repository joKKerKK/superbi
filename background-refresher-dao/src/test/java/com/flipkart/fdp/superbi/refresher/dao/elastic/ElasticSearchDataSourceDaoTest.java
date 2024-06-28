package com.flipkart.fdp.superbi.refresher.dao.elastic;


import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.es.client.ESClient;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.es.client.ESResultSet;
import com.flipkart.fdp.es.client.QueryResultMeta;
import com.flipkart.fdp.models.ClientTypeSignature;
import com.flipkart.fdp.models.Column;
import com.flipkart.fdp.models.ResultRow;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.exceptions.SuperBiException;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class ElasticSearchDataSourceDaoTest {

  private static ObjectMapper MAPPER = new ObjectMapper();
  private static final ProtocolVersion HTTP_PROTOCOL = new ProtocolVersion("http", 1, 1);
  private static final RequestLine REQUEST_LINE = new BasicRequestLine(HttpGet.METHOD_NAME, "/",
      HTTP_PROTOCOL);

  @Mock
  private ESClient esClient;

  @Mock
  private QueryResultMeta queryResultMeta;

  @Mock
  private Column column;

  private final ElasticSearchExceptionMap EXCEPTION_MAP = new ElasticSearchExceptionMap();

  @Mock
  private ESResultSet esResultSet;

  private com.flipkart.fdp.superbi.refresher.dao.elastic.requests.ESQuery esQueryRequest;

  private final ESQuery esQuery = ESQuery.builder().query(
          MAPPER.readValue("{}", ObjectNode.class)).queryType(
          ESQuery.QueryType.AGGR).index("index").type("data").limit(1000)
      .columns(ImmutableList.copyOf(new ArrayList<>()))
      .schema(ImmutableList.copyOf(new ArrayList<>()))
      .build();
  private final DataSourceQuery dataSourceQuery;

  public ElasticSearchDataSourceDaoTest() throws IOException {
    esQueryRequest = Mockito.mock(com.flipkart.fdp.superbi.refresher.dao.elastic.requests.ESQuery.class);
    Mockito.when(esQueryRequest.convertToClientESQuery()).thenReturn(esQuery);
    dataSourceQuery = DataSourceQuery.builder()
        .nativeQuery(esQueryRequest)
        .build();
  }

  @Test
  @SneakyThrows
  public void testStreamingResultSuccess() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);
    List<String> columNames = Lists.newArrayList("COL_1", "COL_2", "COL_3");
    List<Column> columns = columNames.stream()
        .map(colName -> {
          Column column = Mockito.mock(Column.class);
          Mockito.when(column.getName()).thenReturn(colName);
          Mockito.when(column.getDataType()).thenReturn(ClientTypeSignature.INT);
          return column;
        })
        .collect(Collectors.toList());

    Mockito.when(esClient.execute(esQuery)).thenReturn(esResultSet);
    Mockito.when(esResultSet.hasNext()).thenReturn(true);
    Mockito.when(esResultSet.next()).thenReturn(new ResultRow(Arrays.asList(1, 2, 3)));
    Mockito.when(queryResultMeta.getColumns()).thenReturn(columns);
    Mockito.when(esResultSet.getMetadata()).thenReturn(queryResultMeta);

    QueryResult queryResult = dataSourceDao.getStream(dataSourceQuery);

    Assert.assertEquals(queryResult.iterator().hasNext(), esResultSet.hasNext());
    Assert.assertEquals(queryResult.iterator().next(), Arrays.asList(1, 2, 3));
    Assert.assertEquals(queryResult.getColumns(), columNames);
  }

  @Test(expected = SuperBiException.class)
  @SneakyThrows
  public void testExceptionThrownFromClient() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);

    Mockito.when(esClient.execute(esQuery)).thenThrow(new RuntimeException());

    dataSourceDao.getStream(dataSourceQuery);
  }

  @SneakyThrows
  @Test
  public void testElasticSearchMaxBucketsResponseException() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);
    ResponseException responseException = getMaxBucketsResponseException();
    Mockito.when(esClient.execute(esQuery)).thenThrow(new SuperBiException(responseException));

    try {
      dataSourceDao.getStream(dataSourceQuery);
    } catch (Exception e) {
      assertThat(e, instanceOf(ClientSideException.class));
      assertThat(e.getCause(), instanceOf(ResponseException.class));
    }
  }

  @SneakyThrows
  @Test
  public void testElasticSearchResponseException() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);
    ResponseException responseException = getSomeOtherElasticSearchExcption();
    Mockito.when(esClient.execute(esQuery)).thenThrow(new SuperBiException(responseException));

    try {
      dataSourceDao.getStream(dataSourceQuery);
    } catch (Exception e) {
      assertThat(e, instanceOf(ClientSideException.class));
      assertThat(e.getCause(), instanceOf(ResponseException.class));
    }
  }

  @Test
  @SneakyThrows
  public void testSomeRandomException() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);
    Mockito.when(esClient.execute(esQuery)).thenThrow(new RuntimeException(new IOException()));

    try {
      dataSourceDao.getStream(dataSourceQuery);
    } catch (Exception e) {
      assertThat(e, instanceOf(ServerSideException.class));
      assertThat(e.getCause(), instanceOf(IOException.class));
    }
  }

  @Test
  @SneakyThrows
  public void testElasticSearchResponseExceptionWithErrorInBodyParsing() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);
    Response response = buildElasticSearchResponse("{}", 500);

    ResponseException responseException = new ResponseException(response);
    Mockito.when(esClient.execute(esQuery)).thenThrow(new SuperBiException(responseException));

    try {
      dataSourceDao.getStream(dataSourceQuery);
    } catch (Exception e) {
      assertThat(e, instanceOf(ServerSideException.class));
      assertThat(e.getCause(), instanceOf(ResponseException.class));
    }
  }

  @Test
  @SneakyThrows
  public void testElasticSearchResponseExceptionWithoutResponseBody() {
    ElasticSearchDataSourceDao dataSourceDao = new ElasticSearchDataSourceDao(esClient);
    Response response = buildElasticSearchResponse("{}", 500);

    // Remove HttpEntity from response
    Mockito.when(response.getEntity()).thenReturn(null);

    ResponseException responseException = new ResponseException(response);
    Mockito.when(esClient.execute(esQuery)).thenThrow(new SuperBiException(responseException));

    try {
      dataSourceDao.getStream(dataSourceQuery);
    } catch (Exception e) {
      assertThat(e, instanceOf(ServerSideException.class));
      assertThat(e.getCause(), instanceOf(ResponseException.class));
    }
  }

  @SneakyThrows
  private ResponseException getSomeOtherElasticSearchExcption() {
    String body = "{\n"
        + "  \"error\" : {\n"
        + "    \"root_cause\" : [\n"
        + "      {\n"
        + "        \"type\" : \"index_not_found_exception\",\n"
        + "        \"reason\" : \"no such index [test_index123123]\",\n"
        + "        \"resource.type\" : \"index_or_alias\",\n"
        + "        \"resource.id\" : \"test_index123123\",\n"
        + "        \"index_uuid\" : \"_na_\",\n"
        + "        \"index\" : \"test_index123123\"\n"
        + "      }\n"
        + "    ],\n"
        + "    \"type\" : \"index_not_found_exception\",\n"
        + "    \"reason\" : \"no such index [test_index123123]\",\n"
        + "    \"resource.type\" : \"index_or_alias\",\n"
        + "    \"resource.id\" : \"test_index123123\",\n"
        + "    \"index_uuid\" : \"_na_\",\n"
        + "    \"index\" : \"test_index123123\"\n"
        + "  },\n"
        + "  \"status\" : 404\n"
        + "}";

    return buildElasticSearchResponseException(body, 404);
  }

  @SneakyThrows
  private ResponseException getMaxBucketsResponseException() {
    String body = "{\n"
        + "  \"error\": {\n"
        + "    \"root_cause\": [\n"
        + "      {\n"
        + "        \"type\": \"too_many_buckets_exception\",\n"
        + "        \"reason\": \"Trying to create too many buckets. Must be less than or equal "
        + "to: [10000] but was [10001]. This limit can be set by changing the [search"
        + ".max_buckets] cluster level setting.\",\n"
        + "        \"max_buckets\": 10000\n"
        + "      }\n"
        + "    ],\n"
        + "    \"type\": \"search_phase_execution_exception\",\n"
        + "    \"reason\": \"\",\n"
        + "    \"phase\": \"fetch\",\n"
        + "    \"grouped\": true,\n"
        + "    \"failed_shards\": [\n"
        + "      {\n"
        + "        \"shard\": 0,\n"
        + "        \"index\": \"test_index\",\n"
        + "        \"node\": \"FQOizwIVT9KS3P2anBlbsg\",\n"
        + "        \"reason\": {\n"
        + "          \"type\": \"too_many_buckets_exception\",\n"
        + "          \"reason\": \"Trying to create too many buckets. Must be less than or equal "
        + "to: [10000] but was [10001]. This limit can be set by changing the [search"
        + ".max_buckets] cluster level setting.\",\n"
        + "          \"max_buckets\": 10000\n"
        + "        }\n"
        + "      }\n"
        + "    ],\n"
        + "    \"caused_by\": {\n"
        + "      \"type\": \"too_many_buckets_exception\",\n"
        + "      \"reason\": \"Trying to create too many buckets. Must be less than or equal to: "
        + "[10000] but was [10001]. This limit can be set by changing the [search.max_buckets] "
        + "cluster level setting.\",\n"
        + "      \"max_buckets\": 10000\n"
        + "    }\n"
        + "  },\n"
        + "  \"status\": 503\n"
        + "}";

    return buildElasticSearchResponseException(body, 503);
  }

  @SneakyThrows
  private ResponseException buildElasticSearchResponseException(String body, int status) {
    return new ResponseException(buildElasticSearchResponse(body, status));
  }

  private Response buildElasticSearchResponse(String body, int status) {
    StatusLine statusLine = Mockito.mock(StatusLine.class);
    Mockito.when(statusLine.getStatusCode()).thenReturn(status);
    HttpEntity entity = new NStringEntity(body, ContentType.APPLICATION_JSON);
    Response response = Mockito.mock(Response.class);
    Mockito.when(response.getStatusLine()).thenReturn(statusLine);
    Mockito.when(response.getEntity()).thenReturn(entity);
    Mockito.when(response.getRequestLine()).thenReturn(REQUEST_LINE);

    return response;
  }
}
