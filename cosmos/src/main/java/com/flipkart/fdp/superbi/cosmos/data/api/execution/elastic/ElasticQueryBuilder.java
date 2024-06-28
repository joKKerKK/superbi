package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.MATCH_ALL;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.RANGE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.SCRIPT;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig
    .getColumnName;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.logicalOps;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.predicates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.dsl.query.*;
import com.flipkart.fdp.superbi.dsl.query.Predicate.Type;
import com.flipkart.fdp.superbi.dsl.query.exp.*;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.json.JSONException;
import org.slf4j.LoggerFactory;

public abstract class ElasticQueryBuilder extends AbstractQueryBuilder {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticQueryBuilder.class);

    protected static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    private final ArrayNode rootFilterNode = jsonNodeFactory.arrayNode();
    protected final ElasticParserConfig castedConfig;

    protected final List<String> cols;
    protected final List<String> aliases;
    protected final String index;
    protected final String type;
    protected Optional<Integer> limit = Optional.absent();


    public ElasticQueryBuilder(DSQuery query, Map<String, String[]> values, ElasticParserConfig config) {
        super(query, values, config);
        this.castedConfig = config;
        String[] indexAndType = this.castedConfig.getIndexAndType(query);

        cols = Lists.newArrayList();
        aliases = Lists.newArrayList();
        index = indexAndType[0];
        type = indexAndType[1];

        query.getNonDerivedColumns().forEach(col -> cols.add(col.getName().substring(col.getName().lastIndexOf(".")+1)));
        query.getNonDerivedColumns().forEach(col -> aliases.add(col.getAlias()));

    }

    @Override
    public void visit(Optional<Integer> limit) {
        this.limit = limit;
    }


    public ObjectNode getFilterNode() {
        final ObjectNode filterNode = jsonNodeFactory.objectNode();
        if(rootFilterNode.size() == 0)
            filterNode.put(MATCH_ALL, jsonNodeFactory.objectNode());
        else {
            ObjectNode bool = jsonNodeFactory.objectNode();
            bool.put(logicalOps.get(LogicalOp.Type.AND), rootFilterNode);
            filterNode.put("bool", bool);
        }
        return filterNode;
    }

    @Override
    public void visit(DateRangePredicate dateRangePredicate) {
        final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
        dateRangePredicate.accept(filterBuilder);
        rootFilterNode.add(filterBuilder.criteriaNode);
    }

    @Override
    public void visitDateRange(String column, Date start, Date end) {
        final ObjectNode rangeOuterNode = jsonNodeFactory.objectNode();
        final ObjectNode rangeNode = jsonNodeFactory.objectNode();
        rangeOuterNode.put(RANGE, rangeNode);
        final ObjectNode intervalNode = jsonNodeFactory.objectNode();
        intervalNode.put("gte", start.getTime());
        intervalNode.put("lt", end.getTime());
        intervalNode.put("format", "epoch_millis");
        rangeNode.put(getColumnName(column), intervalNode);
        rootFilterNode.add(rangeOuterNode);
    }

    public void visitRange(String column, long start, long end) {
        final ObjectNode rangeOuterNode = jsonNodeFactory.objectNode();
        final ObjectNode rangeNode = jsonNodeFactory.objectNode();
        rangeOuterNode.put(RANGE, rangeNode);
        final ObjectNode intervalNode = jsonNodeFactory.objectNode();
        intervalNode.put("gte", start);
        intervalNode.put("lt", end);
        rangeNode.put(getColumnName(column), intervalNode);
        rootFilterNode.add(rangeOuterNode);
    }

    @Override
    public void visit(Criteria criteria) {
        final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
        criteria.accept(filterBuilder);
        if(!filterBuilder.criteriaNode.toString().equals("{}"))
            rootFilterNode.add(filterBuilder.criteriaNode);
    }

    @Override
    public void visitSample(Integer sample) {
        final ObjectNode limitNode = jsonNodeFactory.objectNode();
        final ObjectNode valueNode = jsonNodeFactory.objectNode();
        valueNode.put("value", sample);
        limitNode.put("limit", valueNode);
        rootFilterNode.add(limitNode);
    }

    @Override
    protected abstract Object buildQueryImpl();

    class PredicateNodeBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor{
        private final Predicate predicate;
        private String columnName;
        private List<String> values = Lists.newArrayList();
        private boolean isParamValueMissing = false;
        boolean hasCompositeColExpression;

        public PredicateNodeBuilder(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public CriteriaVisitor visit(Exp expression) {
            if(expression instanceof CompositeColumnExp && columnName== null) {
                Function<String,String> getColumnForScriptExe = s -> "doc['"+ElasticParserConfig.getColumnName(s) +"'].value";
                columnName = ((CompositeColumnExp)expression).convertToProperExpression(getColumnForScriptExe);
                hasCompositeColExpression = true;
            }
            else if(expression instanceof ColumnExp && columnName== null) {
                columnName = getColumnName(((ColumnExp)expression).evaluateAndGetColName(paramValues));
            }

            return this;
        }

        @Override
        public CriteriaVisitor visit(EvalExp expression) {
            if(expression instanceof LiteralEvalExp) {
                values.add(stringOf(((LiteralEvalExp) expression).value));
            }
            return this;
        }

        @Override
        public CriteriaVisitor visit(Param param) {
            try {
                final Object value = param.getValue(paramValues);
                if(param.isMultiple)
                    values.addAll( ((Collection<?>)value).stream().map(o->stringOf(
                            o)).collect(
                            Collectors.toList()));
                else {
                    values.add(stringOf(value));
                }
            } catch (Exception e) {
                logger.warn(String.format("Filter %s, is ignored in the query since the value is missing. ", param.name)+e.getMessage());
                isParamValueMissing = true;
            }
            return this;

        }

        public JsonNode getNode() {
            if(isParamValueMissing) {
                return jsonNodeFactory.objectNode();
            }
            if(hasCompositeColExpression)
            {
                return buildNodeForCompositeExpressionFilter();
            }
            final ObjectNode node = jsonNodeFactory.objectNode();
            switch (predicate.getType(paramValues)) {
                case not_in:
                case in:
                {
                    final ArrayNode arrayNode = jsonNodeFactory.arrayNode();
                    final Iterator valuesIterator = values.iterator();
                    while(valuesIterator.hasNext()) {
                        arrayNode.add(stringOf(valuesIterator.next()));
                    }
                    node.put(columnName, arrayNode);
                    break;
                }
                case eq:
                {
                    node.put(columnName, values.get(0));
                    break;
                }
                case lt:
                case lte:
                case gt:
                case gte:
                {
                    final ObjectNode rangeNode = jsonNodeFactory.objectNode();
                    rangeNode.put(predicate.getType(paramValues).name(), values.get(0)); //binary range predicate
                    node.put(columnName, rangeNode);
                    break;
                }
                case native_filter:
                    final ArrayNode arrayNode = jsonNodeFactory.arrayNode();
                    String expression = String.valueOf(values.get(0));
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = null;
                    try {
                        jsonNode = mapper.readTree(expression);
                    } catch (IOException e) {
                        String message = String
                            .format("Exception occurred while parsing native expression: %s with alias: %s to Json.", expression, columnName);
                        logger.error(message);
                        throw new JSONException(message, e);
                    }
                    arrayNode.add(jsonNode);
                    node.put("must", arrayNode);
                    break;
                case date_range:
                {
                    final DateRange range;
                    try {
                        range = ((DateRangePredicate)predicate).evaluate(paramValues);
                    } catch (ExprEvalException e) {
                        throw new RuntimeException("Date ranges are invalid");
                    }
                    final ObjectNode dateRangeNode = jsonNodeFactory.objectNode();
                    dateRangeNode.put("gte", range.getStart().getTime());
                    if(values.get(1) != null)
                        dateRangeNode.put("lt", range.getEnd().getTime());
                    dateRangeNode.put("format", "epoch_millis");
                    node.put(columnName, dateRangeNode);
                    break;
                }
                case neq:
                {
                    final ObjectNode equalNode = jsonNodeFactory.objectNode();
                    equalNode.put(columnName, values.get(0));
//                    final ObjectNode filterWrapper = jsonNodeFactory.objectNode();
                    node.put(predicates.get(Predicate.Type.eq), equalNode);
//                    node.put(FILTER, filterWrapper);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("There are no handlers for the predicate " + predicate.getType(paramValues));
            }
            return node;
        }
        private JsonNode buildNodeForCompositeExpressionFilter()
        {
            final ObjectNode scriptFilterNode = jsonNodeFactory.objectNode();
            Map<Predicate.Type,String> operatorsBySymbol = ImmutableMap.<Predicate.Type,String>builder()
                    .put(Predicate.Type.eq, "==")
                    .put(Predicate.Type.neq, "!=")
                    .put(Predicate.Type.gt, ">")
                    .put(Predicate.Type.lt, "<")
                    .put(Predicate.Type.gte,">=")
                    .put(Predicate.Type.lte, "<=").build();
            scriptFilterNode.put(SCRIPT, columnName + " " + operatorsBySymbol.get(
                            predicate.getType(paramValues)) + " " + values.get(
                            0));
            return scriptFilterNode;
        }
        private String getKeyForPredicateNode() {
            if(hasCompositeColExpression)
                return SCRIPT;
            if(predicate.getType(paramValues).equals(Predicate.Type.neq) ||
                predicate.getType(paramValues).equals(Type.not_in))
                    return logicalOps.get(LogicalOp.Type.NOT);
            return predicates.get(predicate.getType(paramValues));
        }
    }

        class RootCriteriaBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

        private final ObjectNode criteriaNode = JsonNodeFactory.instance.objectNode();

        @Override
        public CriteriaVisitor visit(Predicate predicate) {
            final PredicateNodeBuilder localBuilder = new PredicateNodeBuilder(predicate);
            predicate.accept(localBuilder);
            final String key = localBuilder.getKeyForPredicateNode();
            final JsonNode node = localBuilder.getNode();
            if(node.size() != 0) {
                /**
                 * This code has to be refactored. The design has gone wrong
                 */
                if (key.equalsIgnoreCase("must_not")) {
                    final ObjectNode mustNotNode = jsonNodeFactory.objectNode();
                    mustNotNode.put("must_not", node);
                    criteriaNode.put("bool", mustNotNode);
                } else {
                    criteriaNode.put(key, node);
                }
            }
            return new DefaultCriteriaVisitor();
        }

        @Override
        public CriteriaVisitor visit(LogicalOp logicalOp) {

            final ArrayNode criteriaNodes = jsonNodeFactory.arrayNode();
//            criteriaNodes.add(jsonNodeFactory.objectNode()); // A dummy criteria if none of the criteria are valid (coz of invalid params)
            for (final Criteria criteria : logicalOp.getCriteria()) {
                final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
                criteria.accept(filterBuilder);
                if(filterBuilder.criteriaNode.size()!=0) {
                    criteriaNodes.add(filterBuilder.criteriaNode);
                }
            }
            if(criteriaNodes.size() > 0) {
                JsonNode logicalOpNode;
                switch (logicalOp.getType()) {
                    case NOT:
                        logicalOpNode = criteriaNodes.get(0);
                        break;
                    case AND:
                    case OR:
                        logicalOpNode = criteriaNodes;
                        break;
                    default:
                        throw new UnsupportedOperationException("There are not handlers for this logical operator" + logicalOp.getType());
                }
                ObjectNode bool = jsonNodeFactory.objectNode();
                bool.put(logicalOps.get(logicalOp.getType()), logicalOpNode);
                criteriaNode.put("bool", bool);
            }
            return new DefaultCriteriaVisitor();
        }

    }
    private static String stringOf(Object o)
    {
        if(o instanceof Double || o instanceof Float)
        {
            double d = ((Number)o).doubleValue();
            if(d == (long)d)
            {
                return String.valueOf((long)d);
            }
        }
        else if(o instanceof Date)
        {
            return stringOf(((Date)o).getTime());
        }
        return String.valueOf(o);
    }
}
