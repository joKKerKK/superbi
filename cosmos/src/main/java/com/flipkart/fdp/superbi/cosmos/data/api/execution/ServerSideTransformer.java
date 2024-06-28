package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import com.flipkart.fdp.superbi.cosmos.aspects.LogExecTime;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.scriptEngines.CompiledScriptExecutionEngine;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.scriptEngines.ScriptExecutionStrategy;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.transformations.Cumulation;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.transformations.Growth;
import com.flipkart.fdp.superbi.cosmos.data.query.result.QueryResult;
import com.flipkart.fdp.superbi.cosmos.data.query.result.ResultRow;
import com.flipkart.fdp.superbi.cosmos.data.query.result.StreamingQueryResult;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Table;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn.DateHistogram;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 18/09/14
 */

public class ServerSideTransformer {

    private DSQuery query;
    private List<String> nonDerivedColumns;
    private Map<String, String[]> params;
    private List<DataType> dataTypes;

    static Function<Table.Column, String> getName = new Function<Table.Column, String>() {
        @Nullable
        @Override
        public String apply(Table.Column input) {
            return input.getName();
        }
    };

    private String getColumnName(String qualifiedColName) {
        String[] parts = qualifiedColName.split("\\.");
        return parts[parts.length-1];
    }

    private List<DataType> getDataTypesForColsIn(DSQuery query) {
        final MetaAccessor metaAccessor = MetaAccessor.get();
        final List<DataType> dataTypes = Lists.newArrayList();
        final List<Table.Column> columns = metaAccessor.getEntityColumns(query.getFromTable());
        final List<String> columnNames = Lists.transform(columns, getName);
        for (SelectColumn queryCol : query.getVisibleSelectedColumns()) {
            switch(queryCol.type) {
                case AGGREGATION:
                {
                    SelectColumn.Aggregation aggregation = ((SelectColumn.Aggregation) queryCol);
                    AggregationType aggrType = aggregation.aggregationType;
                    int index = columnNames.indexOf(getColumnName(aggregation.colName));
                    if(index == -1 || aggrType == AggregationType.COUNT || aggrType == AggregationType.DISTINCT_COUNT )
                        //When count or distinct_count is used we are passing the result as it is
                        dataTypes.add(DataType.STRING);
                    else
                        dataTypes.add(columns.get(index).getType());

                }
                    break;
                case DATE_HISTOGRAM:
                {
                    int index = columnNames.indexOf(getColumnName(((DateHistogram)queryCol).columnName));
                    dataTypes.add(index == -1 ? DataType.STRING : columns.get(index).getType());
                }
                break;
                case SIMPLE:
                {int index = columnNames.indexOf(getColumnName(((SelectColumn.SimpleColumn)queryCol).colName));
                    dataTypes.add(index == -1 ? DataType.STRING : columns.get(index).getType());}
                    break;
                case DERIVED:
                default:
                    dataTypes.add(DataType.STRING);
            }
        }
        return dataTypes;
    }

    protected ServerSideTransformer(DSQuery query, Map<String, String[]> params, List<String> nonDerivedColumns) {
        this.query = query;
        this.params = params;
        this.nonDerivedColumns = Lists.newArrayList(Iterables.transform(query.getNonDerivedColumns(), SelectColumn.F.name));
        this.dataTypes = getDataTypesForColsIn(query);
    }

    protected ServerSideTransformer() {
    }

    /**
     * @brief: Construct and evaluate the javascript expression for result
     * @return: Return the result of evaluated expression
     */
    private List<Object> imputeDerivedColumns(ScriptExecutionStrategy scriptExecutionStrategy, List<Object> row) {

        if (query.getDerivedColumns() == null || !query.getDerivedColumns().iterator().hasNext()) {
            return row;
        }

        Map<String, Object> variableBinding = new HashMap<>();

        for (int i = 0; i < nonDerivedColumns.size(); i++) {
            String key = nonDerivedColumns.get(i);
            Object obj = row.get(i);
            variableBinding.put(key, obj);
        }

        final List<SelectColumn> selectColumnList = Lists.newArrayList(query.getSelectedColumns());
        for (SelectColumn derivedColumn : query.getDerivedColumns())
            row.add(selectColumnList.indexOf(derivedColumn), scriptExecutionStrategy.execute(derivedColumn.getAlias(),
                    ((SelectColumn.Expression)derivedColumn).expressionString, variableBinding));

        return row;
    }

    public List<Object> postProcessSingleRow(ScriptExecutionStrategy scriptExecutionStrategy, List<Object> row2) {
        List<Object> row = imputeDerivedColumns(scriptExecutionStrategy, row2);
        row = hideColumns(row);
        applyCustomValueFormatting(row);
        return row;
    }

    private void applyCustomValueFormatting(List<Object> row) {
        int i = 0;
        for(final Object obj: row) {
            int index = i++;

            if(obj == null) {
                row.set(index, "");
                continue;
            }

            DataType dataType;
            try {
                dataType = dataTypes.get(index);
            } catch (ArrayIndexOutOfBoundsException e) {
                continue;
            }

            if(dataType.equals(DataType.DATE) ||
                    dataType.equals(DataType.DATETIME) ||
                    dataType.equals(DataType.INTEGER)) {
                try {
                    Object formattedValue = dataType.format(obj);
                    row.set(index, formattedValue);
                } catch(NumberFormatException e) {}
            }
        }
    }
    private List<Object> hideColumns(List<Object> row)
    {
        List<Integer> indicesToHide = Lists.newArrayList();
        final List<SelectColumn> selectedColumns = Lists.newArrayList( query.getSelectedColumns());

        List<Object> modifiedRow = Lists.newArrayList();

        try {
            for (int i = 0; i < selectedColumns.size(); i++) {
                if (selectedColumns.get(i).isVisible()) {
                    modifiedRow.add(row.get(i));
                }
            }
        } catch (Exception e) {
            throw new ClientSideException(e);
        }

        return modifiedRow;
    }

    public Schema buildSchema() {
        final Schema schema = query.getSchema(params, true);
        int i=0;
        for(SelectColumn col: schema.columns) {
            col.setDataType(dataTypes.get(i++));
        }
        return schema;
    }
    @LogExecTime
    public QueryResult postProcess(QueryResult result) {
        /**
         * Transformations are applicable only in batch fetch
         * This is because in stream fetch you will not have the entire data in memory
         */
        try (CompiledScriptExecutionEngine scriptExecutionStrategy = new CompiledScriptExecutionEngine()) {
            applyTransformations(result);
            if (result.data != null) {
                for (ResultRow row : result.data) {
                    row.row = postProcessSingleRow(scriptExecutionStrategy, row.row);
                }
            }
        }

        return new QueryResult(buildSchema(),
                result.data, result.getResultQueryStatsOptional());
    }

    private void applyTransformations(QueryResult result) {
        /**
         * TODO extend cumulation when there are group bys along with histogram
         */
        if(query.getDateHistogramCol().isPresent() && !query.hasGroupBys()) {
            SelectColumn.DateHistogram dateHistogram = query.getDateHistogramCol().get();
            if(dateHistogram.getSeriesType(params).equals(SelectColumn.SeriesType.CUMULATIVE)) {
                result = new Cumulation().apply(query, params, result);
            } else if(dateHistogram.getSeriesType(params).equals(SelectColumn.SeriesType.GROWTH)) {
                result = new Growth().apply(query, params, result);
            }
        }

    }

    public StreamingQueryResult postProcess(final StreamingQueryResult queryResult) {
        final CompiledScriptExecutionEngine scriptExecutionStrategy = new CompiledScriptExecutionEngine();
        final Iterator<List<Object>> iterator = queryResult.iterator();
        return new StreamingQueryResult() {
            @Override
            public Iterator<List<Object>> iterator() {
                return new Iterator<List<Object>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public List<Object> next() {
                        final List<Object> row = iterator.next();
                        return postProcessSingleRow(scriptExecutionStrategy, row);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("TBD");
                    }
                };
            }

            @Override
            public Schema getSchema() {
                return buildSchema();
            }

            @Override
            public void close() {
                scriptExecutionStrategy.close();
                queryResult.close();
            }
        };
    }

    public static ServerSideTransformer getFor(DSQuery query, Map<String, String[]> params,
                                              List<String> schema) {
        return new ServerSideTransformer(query, params, schema);
    }
}