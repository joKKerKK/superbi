package com.flipkart.fdp.superbi.refresher.dao.druid;

import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidQuery;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DruidDataSourceDao implements DataSourceDao {

  private final DruidClient druidClient;

  public DruidDataSourceDao(DruidClient druidClient) {
    this.druidClient = druidClient;
  }

  @Override
  public QueryResult getStream(DataSourceQuery dataSourceQuery) {
    DruidQuery druidQuery = (DruidQuery) dataSourceQuery.getNativeQuery();
    JSONArray result = druidClient.getDataFromQuery(druidQuery);
    return buildQueryResult(result, druidQuery.getHeaderList());
  }

  private QueryResult buildQueryResult(JSONArray res, List<String> headers) throws JSONException {

    return new QueryResult() {

      private Iterator<Object> responseIterator = res.iterator();

      @Override
      public Iterator<List<Object>> iterator() {

        return Iterators.transform(responseIterator, new Function<Object, List<Object>>() {
          @Nullable
          @Override
          public List<Object> apply(@Nullable Object o) {
            JSONObject jsonObject = (JSONObject) o;
            return headers.stream().map(header -> jsonObject.get(header)).collect(Collectors.toList());
          }
        });
      }

      @Override
      public List<String> getColumns() {
        return headers;
      }

      @Override
      public void close() {
        // Ignore
      }
    };
  }
}
