package com.flipkart.fdp.superbi.refresher.dao.elastic.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.es.client.ESQuery.QueryType;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode
public class ESQuery {

  /**
   * index on which the query will get executed
   */
  @JsonProperty
  private final String index;
  /**
   * type on which the query will get executed
   */
  @JsonProperty
  private final String type;
  /**
   * the query
   */
  @JsonProperty
  private final ObjectNode query;
  /**
   * aliases in the query (ordered)
   */
  @JsonProperty
  private final ImmutableList<String> schema;
  /**
   * Columns/expressions in the query (ordered)
   */
  @JsonProperty
  private final ImmutableList<String> columns;
  /**
   * Type of the query, aggregate or term;
   */
  @JsonProperty
  private final QueryType queryType;
  /**
   * size of the query;
   */
  @JsonProperty
  private final int limit;

  public ESQuery(com.flipkart.fdp.es.client.ESQuery query) {
    this(query.getIndex(), query.getType(), query.getQuery(), query.getSchema(),
        query.getColumns(), query.getQueryType(), query.getLimit());
  }

  @JsonCreator
  @Builder
  public ESQuery(@JsonProperty("index") String index, @JsonProperty("type") String type,
      @JsonProperty("query") ObjectNode query,
      @JsonProperty("schema") ImmutableList<String> schema,
      @JsonProperty("columns") ImmutableList<String> columns,
      @JsonProperty("queryType") QueryType queryType, @JsonProperty("limit") int limit) {
    this.index = index;
    this.type = type;
    this.query = query;
    this.schema = schema;
    this.columns = columns;
    this.queryType = queryType;
    this.limit = limit;
  }

  public com.flipkart.fdp.es.client.ESQuery convertToClientESQuery() {
    return com.flipkart.fdp.es.client.ESQuery.builder()
        .index(index)
        .type(type)
        .query(query)
        .schema(schema)
        .columns(columns)
        .queryType(queryType)
        .limit(limit)
        .build();
  }

}