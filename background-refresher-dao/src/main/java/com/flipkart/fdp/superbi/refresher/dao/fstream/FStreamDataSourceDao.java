package com.flipkart.fdp.superbi.refresher.dao.fstream;

import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FStreamQuery;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@AllArgsConstructor
public class FStreamDataSourceDao implements DataSourceDao {

    private final FStreamClient fStreamClient;

    private static final String DIMENSIONS_KEY = "dimensions";
    private static final String AGGREGATE_KEY = "aggregateData";


    @Override
    public QueryResult getStream(DataSourceQuery dataSourceQuery) {
        FStreamQuery fStreamQuery = (FStreamQuery) dataSourceQuery.getNativeQuery();
        JSONArray result = fStreamClient.getAggregatedData(fStreamQuery.getFstreamRequest(), fStreamQuery.getFstreamId());
        return buildQueryResult(result, fStreamQuery);
    }

    private static QueryResult buildQueryResult(JSONArray res, FStreamQuery fStreamQuery) throws JSONException {

        return new QueryResult() {

            private Iterator<Object> responseIterator = res.iterator();

            @Override
            public Iterator<List<Object>> iterator() {
                return Iterators.transform(responseIterator, new Function<Object, List<Object>>() {
                    @Nullable
                    @Override
                    public List<Object> apply(@Nullable Object o) {
                        JSONObject jsonObject = (JSONObject) o;
                        List<Object> row = new ArrayList<>(Arrays.asList(new Object[fStreamQuery.getOrderedFstreamColumns().size()]));
                        if (jsonObject.has(DIMENSIONS_KEY)) {
                            JSONObject dimensions = (JSONObject) jsonObject.get(DIMENSIONS_KEY);
                            dimensions.keys().forEachRemaining(i -> row.set(fStreamQuery.getOrderedFstreamColumns().indexOf(i.split("\\.")[1]), dimensions.get(i)));
                        }
                        JSONObject aggregateData = (JSONObject) jsonObject.get(AGGREGATE_KEY);
                        aggregateData.keys().forEachRemaining(i -> row.set(fStreamQuery.getOrderedFstreamColumns().indexOf(i.split("\\.")[1]), aggregateData.get(i)));
                        return row;
                    }
                });
            }

            @Override
            public List<String> getColumns() {
                return fStreamQuery.getOrderedFstreamColumns();
            }

            @Override
            public void close() {
                // Ignore
            }
        };
    }
}
