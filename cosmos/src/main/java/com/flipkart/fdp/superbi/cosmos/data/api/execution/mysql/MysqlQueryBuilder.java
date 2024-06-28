package com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .getColumnName;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.mysql.MysqlDSLConfig.getWrappedObject;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig
    .getAggregationString;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.vertica.VerticaDSLConfig
    .getPredicateStringFor;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.*;
import com.flipkart.fdp.superbi.dsl.query.exp.*;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.LoggerFactory;

public class MysqlQueryBuilder extends AbstractQueryBuilder {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(MysqlQueryBuilder.class);

    private List<String> selectExpressions = Lists.newArrayList();
    private String from ;
    private final List<String> rootCriteriaString = Lists.newArrayList();
    private List<String> groupByExpressions = Lists.newArrayList();
    private List<String> orderByExpressions = Lists.newArrayList();
    private Optional<Integer> limit = Optional.of(0);

    public MysqlQueryBuilder(DSQuery query, Map<String, String[]> values, MysqlDSLConfig config) {
        super(query, values, config);
    }

    @Override
    public void visit(SelectColumn.SimpleColumn column) {
        selectExpressions.add(
                new StringBuffer()
                    .append(column.colName)
                    .append(" as `")
                    .append(column.getAlias())
                    .append("`")
                    .toString()
        );
    }

    @Override
    public void visit(SelectColumn.Aggregation column,
            SelectColumn.AggregationOptions options) {
        selectExpressions.add(
                new StringBuffer()
                    .append(getAggregationString(column.aggregationType))
                        .append("(" + (column.aggregationType.equals(
                                AggregationType.DISTINCT_COUNT) ?
                                "distinct " :
                                ""))
                    .append(column.colName)
                    .append(")")
                    .append(" as `")
                    .append(column.getAlias())
                    .append("`")
                    .toString()
        );
    }

    @Override
    public void visitFrom(String fromTable) {
        this.from = fromTable;
    }

    /*@Override
    public void visit(DateRangePredicate dateRangePredicate) {
        final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
        dateRangePredicate.accept(filterBuilder);
        rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
    }*/

    @Override
    public void visit(Criteria criteria) {
        final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
        criteria.accept(filterBuilder);
        rootCriteriaString.add(filterBuilder.criteriaBuilder.toString());
    }

    @Override
    public void visitGroupBy(String groupByColumn) {
        groupByExpressions.add(groupByColumn);
    }

    @Override
    public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
        String bucketExpression = "(floor(" + columnName +  "/" + interval + ")" + "*" + interval + ")";
        String histogramExpr = " " + bucketExpression +  " as " + alias + " ";
        selectExpressions.add(histogramExpr);
        // Alias of the bucket expression does not work in where clause for some reason - figure out a way
        rootCriteriaString.add(bucketExpression + ">=" + from);
        rootCriteriaString.add(bucketExpression + "<=" + to);
        groupByExpressions.add(bucketExpression);
        orderByExpressions.add(bucketExpression);
    }

    @Override
    public void visitDateHistogram(String alias, String columnName, Date from, Date to, long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {
        visitDateRange(columnName, from, to);
        selectExpressions.add("min(FROM_UNIXTIME(" + columnName + "/1000)) as `" + alias + "`");
        groupByExpressions.add(" round(FROM_UNIXTIME(" + columnName
                + "/" + intervalMs + "))");
    }

    @Override
    public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
        orderByExpressions.add(orderByColumn + " " + MysqlDSLConfig.getOrderByType(type));
    }


    @Override
    public void visit(Optional<Integer> limit) {
        this.limit = limit;
    }

    @Override
    public void visitDateRange(String column, Date start, Date end) {
        final StringBuilder dateRangeBuilder = new StringBuilder(column);
        dateRangeBuilder.append(">=")
                .append(start.getTime());
        if(end != null) {
            dateRangeBuilder .append(" and ")
                    .append(column)
                    .append("<")
                    .append(end.getTime());
        }
        rootCriteriaString.add(dateRangeBuilder.toString());
    }

    @Override
    protected Object buildQueryImpl() {

        final StringBuilder queryBuffer = new StringBuilder();

        queryBuffer.append("select ");
        Joiner.on(",").appendTo(queryBuffer, selectExpressions);

        queryBuffer.append(" from ").append(from);

        if(!rootCriteriaString.isEmpty()) {
            queryBuffer.append(" where ");
            Joiner.on(" AND ").appendTo(queryBuffer, rootCriteriaString);
        }

        if(!groupByExpressions.isEmpty()) {
            queryBuffer.append(" group by ");
            Joiner.on(",").appendTo(queryBuffer, groupByExpressions);
        }

        if(!orderByExpressions.isEmpty()) {
            queryBuffer.append(" order by ");
            Joiner.on(",").appendTo(queryBuffer, orderByExpressions);
        }

        if(limit.isPresent()) {
            queryBuffer
                    .append(" limit ")
                    .append(limit.get());
        }

        return queryBuffer.toString();
    }

    class PredicateNodeBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor{
        private final Predicate predicate;
        private String columnName;
        private List<Object> values = Lists.newArrayList();
        private boolean isParamValueMissing = false;

        public PredicateNodeBuilder(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public CriteriaVisitor visit(Exp expression) {
            if(expression instanceof ColumnExp && columnName== null) {
                columnName = getColumnName(((ColumnExp)expression).evaluateAndGetColName(paramValues));
            }
            return this;
        }

        @Override
        public CriteriaVisitor visit(EvalExp expression) {
            if(expression instanceof LiteralEvalExp) {
                values.add((String) ((LiteralEvalExp)expression).value);
            }
            return this;
        }

        @Override
        public CriteriaVisitor visit(Param param) {
            try {
                final Object value = param.getValue(paramValues);
                if(param.isMultiple)
                    values.addAll((List<String>)value);
                else {
                    values.add(String.valueOf(value));
                }
            } catch (Exception e) {
                logger.warn(String.format("Filter %s, is ignored in the query since the value is missing.", param.name)+e.getMessage());
                isParamValueMissing = true;
            }
            return this;

        }

        public String getNode() {
            if(isParamValueMissing) {
                return "";
            }
//            final ObjectNode node = jsonNodeFactory.objectNode();
            final StringBuilder predicateBuilder = new StringBuilder();
            predicateBuilder.append(columnName)
                    .append(" ")
                    .append(getPredicateStringFor(predicate.getType(paramValues)));
            switch (predicate.getType(paramValues)) {
                case in:
                {
                    if(values.size() == 0) throw new RuntimeException("At-least one value is expected for if in parameter is passed");
                    predicateBuilder.append("(");
                    Iterable<Object> wrappedObjects = Iterables.transform(values, new Function<Object, Object>() {
                        @Nullable
                        @Override
                        public Object apply(Object input) {
                            return getWrappedObject(input);
                        }
                    });
                    Joiner.on(",").appendTo(predicateBuilder, wrappedObjects);
                    predicateBuilder.append(")");
                    break;
                }
                case native_filter:
                    predicateBuilder.append(values.get(0));
                    break;
                case eq:
                case neq:
                case like:
                case lt:
                case lte:
                case gt:
                case gte:
                {
                    predicateBuilder.append(getWrappedObject(values.get(0)));
                    break;
                }
                default:
                    throw new UnsupportedOperationException("There are no handlers for the predicate " + predicate.getType(paramValues));
            }
            return predicateBuilder.toString();
        }
    }

    class RootCriteriaBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

        private final StringBuilder criteriaBuilder = new StringBuilder();

        @Override
        public CriteriaVisitor visit(Predicate predicate) {
            final PredicateNodeBuilder localBuilder = new PredicateNodeBuilder(predicate);
            predicate.accept(localBuilder);
            String predicateString = localBuilder.getNode();
            if(!(predicateString.equals(""))) {
                criteriaBuilder.append(predicateString);
            }
            return new DefaultCriteriaVisitor();
        }

        @Override
        public CriteriaVisitor visit(LogicalOp logicalOp) {

            criteriaBuilder.append("(");
            final List<String> criteriaNodes = Lists.newArrayList();
            criteriaNodes.add(" 1=1 "); // A dummy criteria if none of the criteria are valid (coz of invalid params)
            for (Criteria criteria : logicalOp.getCriteria()) {
                final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
                criteria.accept(filterBuilder);
                final String criteriaString = filterBuilder.criteriaBuilder.toString();
                if(!criteriaString.equals("")) {
                    criteriaNodes.add(criteriaString);
                }
            }
            switch (logicalOp.getType()) {
                case NOT:
                    criteriaBuilder.append("!(").append(criteriaNodes.get(0)).append(")");
                    break;
                case AND:
                case OR:
                    Joiner.on(" " + logicalOp.getType() + " ").appendTo(criteriaBuilder, criteriaNodes);
                    break;
                default:
                    throw new UnsupportedOperationException("There are not handlers for this logical operator" + logicalOp.getType());
            }
            criteriaBuilder.append(")");
            return new DefaultCriteriaVisitor();
        }

    }

}
