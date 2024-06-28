package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 19/08/14
 */

public enum ElasticQueryType {

    termQuery {
        @Override
        public ElasticQueryBuilder getBuilder(DSQuery query, Map<String, String[]> params, ElasticParserConfig config) {
            return new TermQueryBuilder(query, params, config);
        }

    },

    termAggregateQuery {
        @Override
        public ElasticQueryBuilder getBuilder(DSQuery query, Map<String, String[]> params, ElasticParserConfig config) {
            return new TermAggregateQueryBuilder(query, params, config);
        }
    },



    aggregateQuery {
        @Override
        public ElasticQueryBuilder getBuilder(DSQuery query, Map<String, String[]> params, ElasticParserConfig config) {
            return new AggregateQueryBuilder(query, params, config);
        }
    };

    public abstract ElasticQueryBuilder getBuilder(DSQuery query, Map<String, String[]> params, ElasticParserConfig config);

    public static ElasticQueryType getFor(DSQuery dsQuery) {
        return dsQuery.hasGroupBys() || dsQuery.hasHistograms()?
                aggregateQuery:
                dsQuery.hasAggregations()? termAggregateQuery: termQuery;
    }

}
