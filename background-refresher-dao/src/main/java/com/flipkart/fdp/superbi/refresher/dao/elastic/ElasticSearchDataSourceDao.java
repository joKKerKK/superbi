package com.flipkart.fdp.superbi.refresher.dao.elastic;

import com.flipkart.fdp.es.client.ESClient;
import com.flipkart.fdp.es.client.ESResultSet;
import com.flipkart.fdp.models.Column;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.dao.elastic.requests.ESQuery;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

@Slf4j
@AllArgsConstructor
public class ElasticSearchDataSourceDao implements DataSourceDao {

  private final ESClient client;
  private final ElasticSearchExceptionMap elasticSearchExceptionMap = new ElasticSearchExceptionMap();

  private final Configuration conf = Configuration.defaultConfiguration()
      .addOptions(Option.SUPPRESS_EXCEPTIONS)
      .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

  @Override
  public QueryResult getStream(DataSourceQuery dataSourceQuery) {
    Object nativeQuery = dataSourceQuery.getNativeQuery();
    try {
      ESQuery esQuery = (ESQuery) nativeQuery;
      return toStreamingQueryResult(client.execute(esQuery.convertToClientESQuery()));
    } catch (Exception e) {
      // First unwrap the cause
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      log.error("Query Execution failed for query {} due to {}", nativeQuery,
          cause.getMessage());
      if (!(cause instanceof ResponseException)) {
        //Cause is not a ResponseException, can't do much
        throw new ServerSideException(cause);
      }
      /**
       * Elasticsearch responded and  threw an error
       * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-low
       * -usage-responses.html
       * For now we will treat all ResponseException as ClientSideException as we do not want
       * to open the circuit until we handle each response separately
       */

      ResponseException esException = (ResponseException) cause;
      Response response = esException.getResponse();
      HttpEntity responseEntity = response.getEntity();
      DocumentContext documentContext;
      try {
        documentContext = JsonPath.using(conf).parse(
            EntityUtils.toString(responseEntity));
      }catch (Exception ex) {
        //Could not read the body due to IOException, can't do much.
        log.error("Error in handling the response from ES", ex);
        throw new ServerSideException(cause);
      }


      // .read may send null, so assigning to Integer
      Integer status = documentContext.read("$.status");

      // Check the cause first
      String causeType = documentContext.read("$.error.caused_by.type");
      String causeReason = documentContext.read("$.error.caused_by.reason");

      if (StringUtils.isBlank(causeType)) {
        causeType = documentContext.read("$.error.root_cause[0].type");
      }

      if (StringUtils.isBlank(causeReason)) {
        causeReason = documentContext.read("$.error.root_cause[0].reason");
      }

      throw elasticSearchExceptionMap.buildException(causeType,
          MessageFormat.format("Query failed at ElasticSearch with status {0} : {1} : {2}",
              status, causeType, causeReason), cause);
    }
  }

  private QueryResult toStreamingQueryResult(ESResultSet esResultSet) {
    List<String> columnNames = new ArrayList<>();
    List<Column> columnsMetaData = esResultSet.getMetadata().getColumns();
    for (int i = 0; i < columnsMetaData.size() ; i++ ) {
      columnNames.add(columnsMetaData.get(i).getName());
    }
    final List<String> finalColumnNames = Collections.unmodifiableList(columnNames);

    return new QueryResult() {
      @Override
      public Iterator<List<Object>> iterator() {
        return new Iterator<List<Object>>() {
          @Override
          public boolean hasNext() {
            return esResultSet.hasNext();
          }

          @Override
          public List<Object> next() {
            return esResultSet.next().row;
          }
        };
      }

      @Override
      public List<String> getColumns() {
        return finalColumnNames;
      }

      //no need to close as it will be closed once scroll ends
      @Override
      public void close() {
      }
    };
  }
}
