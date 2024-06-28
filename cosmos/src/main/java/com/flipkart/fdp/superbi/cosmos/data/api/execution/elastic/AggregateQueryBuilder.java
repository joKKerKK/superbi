package com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.AGGS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.DATE_HISTOGRAM;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.FIELD;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.FILTER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.FILTER_WRAPPER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.GMT_5_30;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.HISTOGRAM;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.INTERVAL;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.ORDER;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.QUERY_TIME_OUT_STRING;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.SHARD_SIZE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.SIZE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.TERMS;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.TIME_ZONE;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.aggregations;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.getColumnName;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.getGroupByKeyFromAlias;
import static com.flipkart.fdp.superbi.cosmos.data.api.execution.elastic.ElasticParserConfig.getIntervalStringFrom;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.es.client.ESQuery;
import com.flipkart.fdp.superbi.dsl.query.AggregationType;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AggregateQueryBuilder extends ElasticQueryBuilder {

  private final ObjectNode groupByNode = jsonNodeFactory.objectNode();
  private final ObjectNode aggNode = jsonNodeFactory.objectNode();

  private ObjectNode currentGroupByNode = groupByNode;
  private Map<String, String> groupByKeys = Maps.newHashMap();
  private List<String> orderByKeys = new ArrayList<>();

  public AggregateQueryBuilder(DSQuery query, Map<String, String[]> values,
      ElasticParserConfig config) {
    super(query, values, config);
  }

  @Override
  public void visit(SelectColumn.Aggregation aggregation,
      SelectColumn.AggregationOptions options) {
    final ObjectNode aggCriteria = jsonNodeFactory.objectNode();
    final ObjectNode fieldNode = jsonNodeFactory.objectNode();
    fieldNode.put(FIELD, getColumnName(aggregation.colName));
    aggCriteria.put(aggregations.get(aggregation.aggregationType), fieldNode);
    if (aggregation.aggregationType == AggregationType.DISTINCT_COUNT) {
      fieldNode.put("precision_threshold", 500);
    }
    if (aggregation.aggregationType == AggregationType.FRACTILE) {
      fieldNode.put("percents", jsonNodeFactory.arrayNode().add(options.fractile.get() * 100));
    }
    aggNode.put(getGroupByKeyFromAlias(aggregation.getAlias()), aggCriteria);

    //Only aggregate columns are candidates for ordering
    orderByKeys.add(aggregation.getAlias());
  }


  @Override
  public void visit(SelectColumn.SimpleColumn column) {
    groupByKeys.put(column.colName, column.getAlias());
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    final ObjectNode aggNode = jsonNodeFactory.objectNode();
    currentGroupByNode.put(AGGS, aggNode);
    final ObjectNode groupByNode = jsonNodeFactory.objectNode();
    aggNode.put(groupByKeys.get(groupByColumn), groupByNode);
    final ObjectNode termsNode = jsonNodeFactory.objectNode();
    groupByNode.put(TERMS, termsNode);
    termsNode.put(FIELD, getColumnName(groupByColumn));
    termsNode.put(SIZE, castedConfig.getAggrBucketSize());
    termsNode.put(SHARD_SIZE, castedConfig.getShardSize());
    currentGroupByNode = groupByNode;
  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    final ObjectNode termsNode = (ObjectNode) currentGroupByNode.get(TERMS);
    if (termsNode != null && termsNode.get(ORDER) == null && orderByKeys.contains(orderByColumn)) {
      final ObjectNode orderByNode = jsonNodeFactory.objectNode();
      orderByNode.put(orderByColumn, type.toString());
      termsNode.put(ORDER, orderByNode);
    }
  }

  @Override
  public void visit(Optional<Integer> limit) {
    final ObjectNode termsNode = (ObjectNode) currentGroupByNode.get(TERMS);
    if (termsNode != null && limit.isPresent()) {
      /**
       * This always set a SIZE as 1Lakh on the last groupby node. Hence increasing number of buckets for the last groupby node by a factor of 10
       * All other groupby nodes gets SIZE as castedConfig.getAggrBucketSize()
       * Do we need it this way?
       * TODO Do we need it this way?
       */
      termsNode.put(SIZE, limit.get());
    }
  }

  @Override
  public void visitDateHistogram(String alias, String columnName, Date from, Date to,
                                 long intervalMs, SelectColumn.DownSampleUnit downSampleUnit) {
    visitDateRange(columnName, from, to);
    final ObjectNode aggNode = jsonNodeFactory.objectNode();
    currentGroupByNode.put(AGGS, aggNode);
    currentGroupByNode = aggNode;
    final ObjectNode downSampleNodeWrapper = currentGroupByNode;
    final ObjectNode downsampleOuterNode = jsonNodeFactory.objectNode();
    final ObjectNode downSampleNode = jsonNodeFactory.objectNode();
    downSampleNodeWrapper.put(getGroupByKeyFromAlias(alias), downsampleOuterNode);
    downsampleOuterNode.put(DATE_HISTOGRAM, downSampleNode);
    downSampleNode.put(FIELD, getColumnName(columnName));
    downSampleNode.put(INTERVAL, getIntervalStringFrom(intervalMs));
    downSampleNode.put(TIME_ZONE, GMT_5_30);
    currentGroupByNode = downsampleOuterNode;
  }

  @Override
  public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
    visitRange(columnName, from, to);
    final ObjectNode aggNode = jsonNodeFactory.objectNode();
    currentGroupByNode.put(AGGS, aggNode);
    currentGroupByNode = aggNode;
    final ObjectNode downSampleNodeWrapper = currentGroupByNode;
    final ObjectNode downsampleOuterNode = jsonNodeFactory.objectNode();
    final ObjectNode downSampleNode = jsonNodeFactory.objectNode();
//        final ObjectNode boundsNode = jsonNodeFactory.objectNode();
//        boundsNode.put(HISTOGRAM_MAX, to);
    downSampleNodeWrapper.put(getGroupByKeyFromAlias(alias), downsampleOuterNode);
    downsampleOuterNode.put(HISTOGRAM, downSampleNode);
    downSampleNode.put(FIELD, getColumnName(columnName));
    downSampleNode.put(INTERVAL, interval);
//        downSampleNode.put(BOUNDS, boundsNode);
    currentGroupByNode = downsampleOuterNode;
  }

  @Override
  protected Object buildQueryImpl() {
    final ObjectNode node = jsonNodeFactory.objectNode();
    // We are not interested in search hits, we just need aggregations
    node.put(SIZE, 0);
    node.put(QUERY_TIME_OUT_STRING, castedConfig.getQueryTimeOutMs() + "ms");
    final ObjectNode outerAggsNode = jsonNodeFactory.objectNode();
    node.put(AGGS, outerAggsNode);
//        final ObjectNode filterWrapperNode = jsonNodeFactory.objectNode();
    outerAggsNode.put(FILTER_WRAPPER, groupByNode);
    groupByNode.put(FILTER, getFilterNode());
    currentGroupByNode.put(AGGS, aggNode);
//        filterWrapperNode(AGGS, groupByNode);
    return ESQuery.builder()
        .query(node)
        .queryType(ESQuery.QueryType.AGGR)
        .index(index)
        .type(type)
        .limit(limit.isPresent() ? limit.get() : -1)
        .columns(ImmutableList.copyOf(cols))
        .schema(ImmutableList.copyOf(aliases))
        .build();
  }
}