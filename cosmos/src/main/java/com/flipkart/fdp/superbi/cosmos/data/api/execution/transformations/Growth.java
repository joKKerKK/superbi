package com.flipkart.fdp.superbi.cosmos.data.api.execution.transformations;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;

/**
 * Created by kshitij.rastogi on 12/04/16.
 */
public class Growth implements ITranformer {

    public static double calculate(List<Double> values) {
        if (values.size() == 1) {
            return 0;
        }
        int size = values.size();
        return (values.get(size - 1) - values.get(size - 2)) / values.get(size - 2) * 100;
    }

    @Override
    public QueryResult apply(DSQuery query, Map<String, String[]> params, QueryResult result) {
        final List<SelectColumn> columnList = Lists.newArrayList(query.getNonDerivedColumns());
        final List<List<Double>> values = Lists.newArrayList();
        for(SelectColumn col : columnList) {
            values.add(Lists.<Double>newArrayList());
        }

        for(ResultRow row : result.data) {
            int i=0;
                for(Object col : row.row) {
                final SelectColumn selectColumn = columnList.get(i);
                if(selectColumn instanceof SelectColumn.Aggregation) {
                    values.get(i).add(Double.valueOf(String.valueOf(col)));
                    row.row.set(
                            i,
                            Growth
                                    .calculate(
                                            values.get(i)
                                    )
                    );
                }
                i++;
            }
        }

        return result;
    }
}
