package com.flipkart.fdp.superbi.cosmos.data.api.execution.transformations;

import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by amruth.s on 17/12/14.
 */
public class Cumulation implements ITranformer {

    enum Type {
        SUM_OR_COUNT {
            @Override
            public double cumulate(List<Double> values) {
                /**
                 * 90% of the use cases are sum/count, so trading off readablity for perf
                 * The 1st index will always have the sum and the 2nd value will have the new entry
                 */
                if(values.size() == 1) {
                    return values.get(0);
                }
                double sum = 0;
                for(final Double d : values)
                    sum += d;
                values.set(0, sum);
                values.remove(1);
                return sum;
            }
        },
        AVG {
            @Override
            public double cumulate(List<Double> values) {
                return SUM_OR_COUNT.cumulate(values)/values.size();
            }
        },
        MIN {
            @Override
            public double cumulate(List<Double> values) {
                return Collections.min(values);
            }
        },
        MAX {
            @Override
            public double cumulate(List<Double> values) {
                return Collections.max(values);
            }
        };

        public abstract double cumulate(List<Double> values);
        public static Type getFor(AggregationType type) {
            switch (type) {
                case AVG:
                    return AVG;
                case SUM:
                case COUNT:
                    return SUM_OR_COUNT;
                case MIN:
                    return MIN;
                case MAX:
                    return MAX;
                default:
                    return SUM_OR_COUNT;
            }
        }
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
                            Cumulation
                                    .Type
                                    .getFor(((SelectColumn.Aggregation) selectColumn).aggregationType)
                                    .cumulate(
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
