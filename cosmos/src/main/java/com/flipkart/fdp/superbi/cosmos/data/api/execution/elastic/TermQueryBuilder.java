package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.*;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.QUERY;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .QUERY_TIME_OUT_STRING;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.SIZE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .STORED_FIELDS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .getColumnName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.es.client.ESQuery;
import com.google.common.collect.ImmutableList;
import java.util.Map;

public class TermQueryBuilder extends ElasticQueryBuilder {

    final ArrayNode fields = jsonNodeFactory.arrayNode();

    public TermQueryBuilder(DSQuery query, Map<String, String[]> paramValues, ElasticParserConfig config) {
        super(query, paramValues, config);
        /**
         * Setting default limit
         */
        //visit(DEFAULT_LIMIT);
    }

    @Override
    public void visit(SelectColumn.SimpleColumn column) {
        fields.add(getColumnName(column.colName));
    }

    @Override
    protected Object buildQueryImpl() {
        final ObjectNode node = jsonNodeFactory.objectNode();
        node.put(STORED_FIELDS, fields);
        node.put(QUERY_TIME_OUT_STRING, castedConfig.getQueryTimeOutMs() + "ms");
        node.put(QUERY, getFilterNode());
        if(limit.isPresent()) {
            node.put(SIZE, limit.get());
        }
        return ESQuery.builder()
            .query(node)
            .index(index)
            .queryType(ESQuery.QueryType.TERM)
            .type(type)
            .limit(limit.isPresent() ? limit.get() : -1)
            .columns(ImmutableList.copyOf(cols))
            .schema(ImmutableList.copyOf(aliases))
            .build();

    }
}