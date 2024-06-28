package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Map;

public class Schema implements Serializable {
  public final ImmutableList<SelectColumn> columns;
  public final ImmutableList<String> groupedBy;
  public final ImmutableList<OrderByExp.OrderedBy> orderedBy;
  public final long dataSetSize;

  public Schema(DSQuery dsQuery, Map<String, String[]> params, boolean skipHiddenColumns) {
    columns = ImmutableList.copyOf(
        skipHiddenColumns ? dsQuery.getVisibleSelectedColumns() : dsQuery.getSelectedColumns());
    groupedBy = ImmutableList
        .copyOf(Lists.transform(dsQuery.getGroupByColumns(), ColumnExp.getTransformer(params)));
    orderedBy = ImmutableList.copyOf(
        Iterables
            .filter(Lists.transform(dsQuery.getOrderByColumns(), OrderByExp.getTransformer(params)),
                Predicates
                    .notNull())
    );
    dataSetSize = dsQuery.getLimit().isPresent() ? dsQuery.getLimit().get() : -1;
  }

  @JsonCreator
  public Schema(@JsonProperty("columns") ImmutableList<SelectColumn> columns,
      @JsonProperty("groupedBy") ImmutableList<String> groupedBy,
      @JsonProperty("orderedBy")
          ImmutableList<OrderByExp.OrderedBy> orderedBy,
      @JsonProperty("dataSetSize") int dataSetSize) {
    this.columns = columns;
    this.groupedBy = groupedBy;
    this.orderedBy = orderedBy;
    this.dataSetSize = dataSetSize;
  }
}