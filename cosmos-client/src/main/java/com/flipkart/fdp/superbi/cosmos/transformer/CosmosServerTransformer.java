package com.flipkart.fdp.superbi.cosmos.transformer;

import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.ServerSideTransformer;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.scriptEngines.CompiledScriptExecutionEngine;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class CosmosServerTransformer {

    private final QueryResult queryResult;
    private final DSQuery dsQuery;
    private final Map<String, String[]> params;
    private final Timer timer;
    private final ServerSideTransformer serverSideTransformer;
    private final List<String> columnNames;

    public CosmosServerTransformer(QueryResult queryResult, DSQuery dsQuery, Map<String,
        String[]> params, Timer timer) {
        this(queryResult, dsQuery, params, timer,ServerSideTransformer.getFor(dsQuery, params,
            null));
    }

    public CosmosServerTransformer(QueryResult queryResult, DSQuery dsQuery, Map<String, String[]> params, Timer timer, ServerSideTransformer serverSideTransformer) {
        this(queryResult, dsQuery, params, timer, serverSideTransformer, getColumnHeaders(dsQuery.getSchema(params)));
    }

    private static List<String> getColumnHeaders(Schema schema) {
        return schema.columns.stream().map(selectCol -> selectCol.getAlias()).collect(Collectors.toList());
    }

    public QueryResult postProcess() {
        final CompiledScriptExecutionEngine scriptExecutionStrategy = new CompiledScriptExecutionEngine();
        final Iterator<List<Object>> transformedSeriesIterator;
        if(dsQuery.getDateHistogramCol().isPresent() && !dsQuery.hasGroupBys()) {

            SelectColumn.DateHistogram dateHistogram = dsQuery.getDateHistogramCol().get();

            if(dateHistogram.getSeriesType(params).equals(SelectColumn.SeriesType.CUMULATIVE)) {
                transformedSeriesIterator = new CumulationResultIterator(queryResult.iterator(),
                    Lists.newArrayList(dsQuery.getNonDerivedColumns()));
            }
            else if(dateHistogram.getSeriesType(params).equals(SelectColumn.SeriesType.GROWTH)) {
                transformedSeriesIterator = new GrowthResultIterator(queryResult.iterator(),
                    Lists.newArrayList(dsQuery.getNonDerivedColumns()));
            }else {
                transformedSeriesIterator = queryResult.iterator();
            }
        }else {
            transformedSeriesIterator = queryResult.iterator();
        }

        return new QueryResult() {
            @Override
            public Iterator<List<Object>> iterator() {
                return Iterators.transform(transformedSeriesIterator, new Function<List<Object>, List<Object>> () {
                    @Nullable
                    @Override
                    public List<Object> apply(@Nullable List<Object> row) {
                        try(Timer.Context context = timer.time()){
                            return serverSideTransformer.postProcessSingleRow(scriptExecutionStrategy, row);
                        }
                    }
                });
            }

            @Override
            public List<String> getColumns() {
                return columnNames;
            }

            @Override
            public void close() {
                try {
                    scriptExecutionStrategy.close();
                } catch(Exception ignore) {
                    // Object pool removes the Abandoned object and
                    // Can return error with this message 'Object has already been returned to this pool or is invalid'
                    // when AbandonObject is returned to the pool
                    log.debug("Error in closing scriptExecutionStrategy", ignore);
                } finally {
                    queryResult.close();
                }
            }
        };
    }
}
