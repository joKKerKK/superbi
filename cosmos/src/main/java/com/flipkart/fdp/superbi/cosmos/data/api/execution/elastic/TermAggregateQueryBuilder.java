package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.*;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.AGGS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.FILTER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .FILTER_WRAPPER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .QUERY_TIME_OUT_STRING;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.SIZE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .getColumnName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.es.client.ESQuery;
import com.google.common.collect.ImmutableList;
import java.util.Map;

/**
 * Created by amruth.s on 18/03/15.
 */
public class TermAggregateQueryBuilder extends ElasticQueryBuilder {

    public TermAggregateQueryBuilder(DSQuery query, Map<String, String[]> values, ElasticParserConfig config) {
        super(query, values, config);
    }

    final ObjectNode aggregations = jsonNodeFactory.objectNode();
    private final ObjectNode aggNode = jsonNodeFactory.objectNode();
    private final ObjectNode filterNode = jsonNodeFactory.objectNode();

    @Override
    public void visit(SelectColumn.Aggregation aggregation,
                      SelectColumn.AggregationOptions options) {
        final String alias = aggregation.getAlias();
        final String colName = getColumnName(aggregation.colName);
        final String aggregationType = ElasticParserConfig.aggregations.get(aggregation.aggregationType);
        final ObjectNode l1Node = jsonNodeFactory.objectNode();
        final ObjectNode l2Node = jsonNodeFactory.objectNode();
        l2Node.put(ElasticParserConfig.FIELD, colName);
        if(aggregation.aggregationType == AggregationType.DISTINCT_COUNT) {
            l2Node.put("precision_threshold", 500);
        }
        if(aggregation.aggregationType == AggregationType.FRACTILE) {
            l2Node.put("percents", jsonNodeFactory.arrayNode().add(options.fractile.get()*100));
        }
        aggregations.put(alias, l1Node);
        l1Node.put(aggregationType, l2Node);
    }

    @Override
    protected Object buildQueryImpl() {
        final ObjectNode node = jsonNodeFactory.objectNode();
        node.put(QUERY_TIME_OUT_STRING, castedConfig.getQueryTimeOutMs()+ "ms");
        node.put(AGGS, aggNode);
        aggNode.put(FILTER_WRAPPER, filterNode);
        filterNode.put(FILTER, getFilterNode());
        filterNode.put(AGGS, aggregations);
        // We are not interested in search hits, we just need aggregations
        node.put(SIZE, 0);
        return ESQuery.builder()
            .query(node)
            .index(index)
            .queryType(ESQuery.QueryType.AGGR)
            .type(type)
            .limit(limit.isPresent() ? limit.get() : -1)
            .columns(ImmutableList.copyOf(cols))
            .schema(ImmutableList.copyOf(aliases))
            .build();

    }
}
