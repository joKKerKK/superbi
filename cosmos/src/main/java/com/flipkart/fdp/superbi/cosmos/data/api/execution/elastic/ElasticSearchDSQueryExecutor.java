package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import static com.flipkart.fdp.superbi.dsl.query.AggregationType.SUM;
import static com.flipkart.fdp.superbi.dsl.query.factory.DSQueryBuilder.select;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.AGGR;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.DATE_HISTOGRAM;
import static com.flipkart.fdp.superbi.dsl.query.factory.ExprFactory.LIT;

import com.beust.jcommander.internal.Maps;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.DSQueryExecutor;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.flipkart.fdp.es.client.ESClient;
import com.flipkart.fdp.es.client.ESClientConfig;
import com.flipkart.fdp.es.client.ESClientException;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.es.client.ESResultSet;
import com.flipkart.fdp.es.client.http.ESHttpClient;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 19/08/14
 */

public class ElasticSearchDSQueryExecutor extends DSQueryExecutor {
    private static ObjectMapper MAPPER =  new ObjectMapper();
    private static final String DEFAULT_FETCH_SIZE = "100";
    private static final String DEFAULT_REQ_TIMEOUT_MS = "120000";
    private static final String DEFAULT_SCROLL_BATCH_SIZE = "1024";
    private static final String DEFAULT_SCROLL_ALIVE_TIME = "1m";
    private final ESClient client;


    public ElasticSearchDSQueryExecutor(final String sourceName, final String sourceType, final String host, final int port,
                                        com.google.common.base.Optional<String> clusterNameOptional,
                                        final Map<String, String> attributes) {
        super(sourceName, sourceType, new ElasticParserConfig(attributes));

        client = new ESHttpClient(ESClientConfig.builder()
                .hosts(Arrays.asList(host.split(",")))
                .clusterName(clusterNameOptional.or(""))
                .requestTimeoutMs(
                    Integer.parseInt(attributes.getOrDefault("requestTimeoutMs", DEFAULT_REQ_TIMEOUT_MS))
                )
                .termFetchSize(Integer.parseInt(attributes.getOrDefault("termFetchSize", DEFAULT_FETCH_SIZE)))
                .scrollBatchSize(Integer.parseInt(attributes.getOrDefault("scrollBatchSize", DEFAULT_SCROLL_BATCH_SIZE)))
                .scrollAliveTime(attributes.getOrDefault("scrollAliveTime", DEFAULT_SCROLL_ALIVE_TIME))
                .build()
        );
    }

    @Override
    @LogExecTime
    public ElasticQueryBuilder getTranslator(DSQuery query, Map<String, String[]> paramValues) {
        return ElasticQueryType.getFor(query).getBuilder(query, paramValues, (ElasticParserConfig) config);
    }

    @Override
    public Object explainNative(Object nativeQuery) {
        try {
            return client.explain((ESQuery) nativeQuery);
        } catch (ESClientException e) {
            throw new RuntimeException(e);
        }
    }

    @LogExecTime
    @Override
    public QueryResult executeNative(Object object, ExecutionContext context) {
        try {
            return toQueryResult(context.query, client.execute((ESQuery) object) );
        } catch (ESClientException e) {
            throw new RuntimeException(e);
        }
    }

    @LogExecTime
    @Override
    public QueryResult executeNative(Object object, ExecutionContext context, ICacheClient<String, QueryResult> cacheClient) {
        return executeNative(object, context);
    }

    @LogExecTime
    @Override
    public StreamingQueryResult executeStreamNative(Object object, ExecutionContext context) {
        try {
            return toStreamingQueryResult(context.query, client.execute((ESQuery) object));
        } catch (ESClientException e) {
            throw new RuntimeException(e);
        }
    }

    public QueryResult toQueryResult(DSQuery query, ESResultSet esResultSet) {
        final List<com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow> rows = Lists.newArrayList();
        while (esResultSet.hasNext()) {
            rows.add(new com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow(esResultSet.next().row));
        }

        return new QueryResult(query.getSchema(Maps.newHashMap()), rows);
    }

    public StreamingQueryResult toStreamingQueryResult(DSQuery query, ESResultSet esResultSet) {

        return new StreamingQueryResult() {
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
            public Schema getSchema() {
                return query.getSchema(Maps.newHashMap());
            }

            @Override
            public void close() {

            }
        };
    }

    public static void main (String ar[]) throws IOException {
        final DSQuery query =
                select(
                        DATE_HISTOGRAM("timestamp", LIT(new Date(1486924200000L)),LIT(new Date(1486924200000L)), LIT(60), LIT("INSTANTANEOUS"), LIT("MINUTE")),
                        AGGR("units", SUM)
                )
                        .from("f_scp_oms::sales_oi_lite_fact")
                        .build();

        ESQuery esQuery = ESQuery.builder()
            .index("f_scp_oms::sales_oi2_fact")
            .type("data")
            .query(MAPPER.readValue("{\n"
                + "  \"size\": 0,\n"
                + "  \"timeout\": 200000,\n"
                + "  \"aggs\": {\n"
                + "    \"filter_wrapper\": {\n"
                + "      \"aggs\": {\n"
                + "        \"timestamp\": {\n"
                + "          \"date_histogram\": {\n"
                + "            \"field\": \"timestamp\",\n"
                + "            \"interval\": \"60.0m\",\n"
                + "            \"time_zone\": \"+05:30\"\n"
                + "          },\n"
                + "          \"aggs\": {\n"
                + "            \"units\": {\n"
                + "              \"sum\": {\n"
                + "                \"field\": \"units\"\n"
                + "              }\n"
                + "            }\n"
                + "          }\n"
                + "        }\n"
                + "      },\n"
                + "      \"filter\": {\n"
                + "        \"and\": [\n"
                + "          {\n"
                + "            \"range\": {\n"
                + "              \"timestamp\": {\n"
                + "                \"gte\": 1486924200000,\n"
                + "                \"lt\": 1487010600000,\n"
                + "                \"format\": \"epoch_millis\"\n"
                + "              }\n"
                + "            }\n"
                + "          },\n"
                + "          {\n"
                + "            \"and\": [\n"
                + "              {\n"
                + "                \"terms\": {\n"
                + "                  \"order_item_true_status\": [\n"
                + "                    \"delivered\",\n"
                + "                    \"returned\",\n"
                + "                    \"shipped\",\n"
                + "                    \"return_requested\",\n"
                + "                    \"ready_to_ship\",\n"
                + "                    \"approved\",\n"
                + "                    \"dispatched\",\n"
                + "                    \"on_hold\"\n"
                + "                  ]\n"
                + "                }\n"
                + "              },\n"
                + "              {\n"
                + "                \"term\": {\n"
                + "                  \"category\": \"Book\"\n"
                + "                }\n"
                + "              },\n"
                + "              {\n"
                + "                \"term\": {\n"
                + "                  \"freebie_flag\": \"false\"\n"
                + "                }\n"
                + "              }\n"
                + "            ]\n"
                + "          },\n"
                + "          {\n"
                + "            \"range\": {\n"
                + "              \"timestamp\": {\n"
                + "                \"gte\": 1486924200000,\n"
                + "                \"lt\": 1487010600000,\n"
                + "                \"format\": \"epoch_millis\"\n"
                + "              }\n"
                + "            }\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}", ObjectNode.class))
            .queryType(ESQuery.QueryType.AGGR)
            .limit(10)
            .schema(ImmutableList.of("timestamp", "units"))
            .columns(ImmutableList.of("timestamp", "units"))
            .build();

        final ElasticSearchDSQueryExecutor executor
                = new ElasticSearchDSQueryExecutor("ES", SourceType.ELASTIC_SEARCH.name(),"10.34.117.187:9200", 9200, Optional.of("prod-fdes-stage"), Maps.newHashMap());
        QueryResult queryResult = executor.executeNative(
                esQuery ,
                new ExecutionContext(query, Maps.newHashMap())
        );
        queryResult.detailedSchema.columns
                .stream()
                .forEach(col -> System.out.print(col.getAlias()+":"+col.getDataType()+"\t"));
        System.out.println("\n");

        queryResult.data
                .stream()
                .forEach(row -> {
                    row.row.stream().forEach(obj -> System.out.print(String.valueOf(obj) + "\t"));
                    System.out.println("\n");
                });

        executor.shutdown();

    }

    private void shutdown() {
        client.close();
    }
}
