package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

import static com.flipkart.fdp.superbi.dsl.query.AggregationType.SUM;
import static com.flipkart.fdp.superbi.dsl.query.factory.DSQueryBuilder.select;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.AGGR;

public class FStreamDSQueryExecutor extends DSQueryExecutor {

  private final FStreamClient fstreamClient;
  private static final String DIMENSIONS_KEY = "dimensions";
  private static final String AGGREGATE_KEY = "aggregateData";

  public FStreamDSQueryExecutor(String sourceName, String sourceType, String host, String port, Map<String, String>
      attributes) {
    super(sourceName, sourceType, new FStreamParserConfig(attributes));
    fstreamClient = new FStreamClient(host, port);
  }

  @Override
  public AbstractQueryBuilder getTranslator(DSQuery query, Map<String, String[]> paramValues) {
    return new FStreamQueryBuilder(query, paramValues, config);
  }

  @Override
  public Object explainNative(Object nativeQuery) {
    return new Object();
  }

  @Override
  public QueryResult executeNative(Object object, ExecutionContext context) {
    FStreamQuery fStreamQuery = (FStreamQuery) object;
    JSONArray response = fstreamClient.getAggregatedData(fStreamQuery.getFstreamRequest(),
        fStreamQuery.getFstreamId());
    try {
      return buildQueryResult(response, context);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main (String ar[])  {
    final DSQuery query =
        select(
            AGGR("cf.lstPrc", SUM),
            AGGR("cf.ingstdAt", SUM),
            AGGR("cf.updtdAt", SUM),
            AGGR("cf.oId", SUM)
        )
            .from("sales_unit_live_face")
            .build();


    final FStreamDSQueryExecutor executor
        = new FStreamDSQueryExecutor("FStream", SourceType.FSTREAM.name(),"10.34.181.215", String.valueOf(2342), Maps
        .newHashMap());
    QueryResult queryResult = executor.executeNative(
        new FStreamQuery(null, null) ,
        new ExecutionContext(query, Maps.newHashMap())
    );
    queryResult.data
        .stream()
        .forEach(row -> {
          row.row.stream().forEach(obj -> System.out.print(String.valueOf(obj) + "\t"));
          System.out.println("\n");
        });
  }

  private QueryResult buildQueryResult(JSONArray res, ExecutionContext context) throws JSONException {
    List<SelectColumn> columns = context.query.getSchema(context.params).columns;
    Map<String, Integer> indexMap = new HashMap<>();
    for (int i=0; i < columns.size(); i++) {
      switch (columns.get(i).type) {
        case SIMPLE:
          SelectColumn.SimpleColumn simpleColumn = (SelectColumn.SimpleColumn) columns.get(i);
          indexMap.put(simpleColumn.colName.split("\\.")[1], i);
          break;
        case AGGREGATION:
          SelectColumn.Aggregation aggregationCol = (SelectColumn.Aggregation) columns.get(i);
          indexMap.put(aggregationCol.colName.split("\\.")[1], i);
          break;
        case DATE_HISTOGRAM:
          SelectColumn.DateHistogram dateHistogram = (SelectColumn.DateHistogram) columns.get(i);
          indexMap.put(dateHistogram.columnName.split("\\.")[1], i);
          break;
        case DERIVED:
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    List<ResultRow> rows = Lists.newArrayList();
    for (int i=0; i < res.length(); i++) {
      ResultRow resultRow = new ResultRow();
      JSONObject jsonObject = res.getJSONObject(i);
      List<Object> row = new ArrayList<>(Arrays.asList(new Object[columns.size()]));
      if (jsonObject.has(DIMENSIONS_KEY)) {
        JSONObject dimensions = (JSONObject)jsonObject.get(DIMENSIONS_KEY);
        for(int dimIdx = 0; dimIdx<dimensions.names().length(); dimIdx++) {
          String colName = dimensions.names().getString(dimIdx).split("\\.")[1];
          row.set(indexMap.get(colName), dimensions.get(dimensions.names().getString(dimIdx)));
        }
      }
      JSONObject aggregateData = (JSONObject)jsonObject.get(AGGREGATE_KEY);
      for(int aggIdx = 0; aggIdx<aggregateData.names().length(); aggIdx++) {
        String colName = aggregateData.names().getString(aggIdx).split("\\.")[1];
        row.set(indexMap.get(colName), aggregateData.get(aggregateData.names().getString(aggIdx)));
      }
      resultRow.row = row;
      rows.add(resultRow);
    }
    return new QueryResult(null, rows);
  }

  @Override
  public QueryResult executeNative(Object object, ExecutionContext context, ICacheClient<String,
        QueryResult> cacheClient) {
    return null;
  }

  @Override
  public StreamingQueryResult executeStreamNative(Object object, ExecutionContext context) {
    return null;
  }
}
