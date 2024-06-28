package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import static java.lang.Math.ceil;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.query.budget.strategy.HeuristicBudgetEstimationStrategy;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Predicate;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.dsl.query.Predicate.Type;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 23/08/14
 */

public class ElasticParserConfig extends AbstractDSLConfig {

    private int aggrBucketSize = 3000;

    @Override
    public Set<LogicalOp.Type> getSupportedLogicalOperators() {
        return logicalOps.keySet();
    }

    @Override
    public Set<AggregationType> getSupportedAggregations() {
        return aggregations.keySet();
    }

    @Override
    public Set<Predicate.Type> getSupportedPredicates() {
        return predicates.keySet();
    }

    public int getAggrBucketSize() {
        return aggrBucketSize;
    }

    @Getter
    private String elasticSearchVersion = "5.X";

    private double shardSizeMultiplier = 1.5;

    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-aggregations-bucket-terms-aggregation.html
     * https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-aggregations-bucket-terms-aggregation.html
     */
    public int getShardSize() {
        return (int) (getAggrBucketSize() * shardSizeMultiplier) + 10;
    }

    public ElasticParserConfig(Map<String, String> overrides) {
        super(overrides);
        if(overrides.containsKey(AGGR_BUCKET_SIZE_PTY)) {
            aggrBucketSize = Integer.valueOf(overrides.get(AGGR_BUCKET_SIZE_PTY));
        }
        if(overrides.containsKey(ELASTICSEARCH_VERSION) && StringUtils.isNotBlank(overrides.get(ELASTICSEARCH_VERSION))) {
            elasticSearchVersion = overrides.get(ELASTICSEARCH_VERSION);
        }
        if(overrides.containsKey(SHARD_SIZE) && StringUtils.isNotBlank(overrides.get(SHARD_SIZE))) {
            shardSizeMultiplier = Double.valueOf(overrides.get(SHARD_SIZE));
        }
        budgetEstimationStrategy = new HeuristicBudgetEstimationStrategy();
    }

    /****************** STATIC PART OF THE CLASS ***********************/

    public static final ImmutableMap<LogicalOp.Type, String> logicalOps;
    public static final ImmutableMap<Predicate.Type, String> predicates;
    public static final ImmutableMap<AggregationType, String> aggregations;


    static {
        final ImmutableMap.Builder<LogicalOp.Type, String> logicalOpsBuilder = ImmutableMap.builder();
        logicalOpsBuilder.put(LogicalOp.Type.AND, "must");
        logicalOpsBuilder.put(LogicalOp.Type.OR, "should");
        logicalOpsBuilder.put(LogicalOp.Type.NOT, "must_not");
        logicalOps = logicalOpsBuilder.build();
        
        final ImmutableMap.Builder<Predicate.Type, String> predicatesBuilder = ImmutableMap.builder();
        predicatesBuilder.put(Predicate.Type.lt, "range");
        predicatesBuilder.put(Predicate.Type.gt, "range");
        predicatesBuilder.put(Predicate.Type.gte, "range");
        predicatesBuilder.put(Predicate.Type.lte, "range");
        predicatesBuilder.put(Predicate.Type.in, "terms");
        predicatesBuilder.put(Predicate.Type.not_in, "terms");
        predicatesBuilder.put(Predicate.Type.eq, "term");
        predicatesBuilder.put(Predicate.Type.date_range, "range");
        predicatesBuilder.put(Predicate.Type.native_filter, "bool");
        predicates = predicatesBuilder.build();

        final ImmutableMap.Builder<AggregationType, String> aggregationsBuilder = ImmutableMap.builder();
        aggregationsBuilder.put(AggregationType.COUNT, "value_count");
        aggregationsBuilder.put(AggregationType.DISTINCT_COUNT, "cardinality");
        aggregationsBuilder.put(AggregationType.MAX, "max");
        aggregationsBuilder.put(AggregationType.SUM, "sum");
        aggregationsBuilder.put(AggregationType.MIN, "min");
        aggregationsBuilder.put(AggregationType.AVG, "avg");
        aggregationsBuilder.put(AggregationType.FRACTILE, "percentiles");
        aggregations = aggregationsBuilder.build();
    }

    public static final String AGGS = "aggs";
    public static final String MATCH_ALL = "match_all";
    public static final String TERMS = "terms";
    public static final String FIELD = "field";
    public static final String FILTER = "filter";
    public static final String FILTER_WRAPPER = "filter_wrapper";
    public static final String FIELDS = "fields";
    public static final String STORED_FIELDS = "stored_fields";
    public static final String QUERY = "query";
    public static final String SIZE = "size";
    public static final String SHARD_SIZE = "shard_size";
    public static final String AGGR_BUCKET_SIZE_PTY = "aggr_bucket_size";
    public static final String ELASTICSEARCH_VERSION = "elasticsearch_version";
    public static final String CLUSTER_NAME_PTY = "cluster.name";
    public static final String DATE_HISTOGRAM = "date_histogram";
    public static final String HISTOGRAM = "histogram";
    public static final String HISTOGRAM_MIN = "min";
    public static final String HISTOGRAM_MAX = "max";
    public static final String BOUNDS = "extended_bounds";
    public static final String INTERVAL = "interval";
    public static final String TIME_ZONE = "time_zone";
    public static final String GMT_5_30 = "+05:30";
    public static final String RANGE = "range";
    public static final String QUERY_TIME_OUT_STRING = "timeout";
    public static final String PERCENTS = "percents";
    public static final String SCRIPT = "script";
    public static final String ORDER = "order";


    public static String getIntervalStringFrom(long intervalInMs) {
        return (int)ceil((intervalInMs / (1000 * 60))) + "m";
    }

    public static String getColumnName(String fullyQualifiedName) {
        // We get the fully qualified name of the column - (index.type.colName) and returns the last part
        String[] colParts = fullyQualifiedName.split("\\.");
        return colParts[colParts.length-1];
    }

    public static String getGroupByKeyFromAlias(String alias) {
        // This is because group by key cannot have
        return alias.replace(" ", "---");
    }

    public static String getGroupByAliasFromKey(String key) {
        return key.replace("---", " ");
    }

    public String[] getIndexAndType(DSQuery query) {
        /**
         * To aid in testing where the from table exactly has the index and type
         */
        if (query.getFromTable().contains("::")) {
            return query.getFromTable().split("::");
        }
        String[] scopeAndTable = getScopeAndTable(query.getFromTable());
        assert scopeAndTable.length == 2;

        String index;
        String type;
        if(this.getElasticSearchVersion().startsWith("7.")) {
            index = String.format("%s--%s", scopeAndTable[0], scopeAndTable[1]);
            type = "_doc";
        }else {
            index = String.format("%s::%s", scopeAndTable[0], scopeAndTable[1]);
            type = "data";
        }

        return new String[] {index, type};
    }

    private static String[] getScopeAndTable(String queryTableName) {
        String[] scopeAndTable = queryTableName.split("\\.");
        if(scopeAndTable.length != 2) {
            scopeAndTable = MetaAccessor.get().getFactByName(queryTableName).getTableName().split("\\.");

            if(scopeAndTable.length != 2)
                throw new RuntimeException("Invalid datasource, index/type info missing " + scopeAndTable);
        }
        return scopeAndTable;
    }
}
