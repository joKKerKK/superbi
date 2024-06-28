package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractDSLConfig;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.AbstractQueryBuilder;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.Aggregate;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.AggregateType;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.FstreamRequest;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.Granularity;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.Range;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.dsl.query.Criteria;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.LogicalOp;
import com.flipkart.fdp.superbi.dsl.query.Param;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.EnumUtils;

public class FStreamQueryBuilder extends AbstractQueryBuilder {

  private Map<String, String> factProperties;
  private static final String FSTREAM_ID_KEY = "fStreamId";
  private static final String OPERATOR_KEY = "$operator$";
  private String COL_PREPEND;
  private static final String COL_PREPEND_KEY = "col_prepend";
  private String GRANULARITY;
  private static final String GRANULARITY_KEY = "granularity";
  private static final String FSTREAM_UNIQUE_AGGREGATE_TYPE = "UNIQUE_COUNT";
  private String id;
  private List<String> simpleColumns;
  private List<String> groupByColumns;
  private List<String> orderByColumns;
  private List<Aggregate> aggregateList;
  private Range range;
  private Map<String, Object> filters = null;
  private Map<String,List<Object>> inFilter = null;

  private boolean isSingleDateRange = false;
  private boolean isAggregateColPresent = false;

  private List<String> fstreamColumnNames = Lists.newArrayList();


  public FStreamQueryBuilder(DSQuery query, Map<String, String[]> paramValues, AbstractDSLConfig
      config) {
    super(query, paramValues, config);
    simpleColumns = Lists.newArrayList();
    groupByColumns = Lists.newArrayList();
    orderByColumns = Lists.newArrayList();
    aggregateList = Lists.newArrayList();
    range = new Range();
    factProperties = new HashMap<>();
    filters = new HashMap<>();
    inFilter = new HashMap<>();
  }

  private int getIndexOfColumn(SelectColumn selectColumn) {
    return this.query.getSchema(paramValues).columns.indexOf(selectColumn);
  }

  @Override
  public void visit(SelectColumn.SimpleColumn column) {
    //Simple columns by itself won't occur in FStream use case so they are not added in aggregates
    final String fstreamColumnName = COL_PREPEND + column.colName.split("\\.")[1];
    simpleColumns.add(fstreamColumnName);
    fstreamColumnNames.add(fstreamColumnName);
//    Aggregate aggregate = new Aggregate();
    //OVERWRITE is default aggregation for simple column
//    aggregate.setAggregateType(new AggregateType(FStreamAggregateType.OVERWRITE.toString()));
//    aggregate.setFieldName(COL_PREPEND + column.colName.split("\\.")[1]);
//    aggregateList.add(aggregate);
  }

  @Override
  public void visit(SelectColumn.Aggregation column,
                    SelectColumn.AggregationOptions options) {
    isAggregateColPresent = true;
    if (options.fractile.isPresent()) {
      throw new UnsupportedOperationException("Fractile is not supported in fStream");
    }
    if (!EnumUtils.isValidEnum(FStreamAggregateType.class, column.aggregationType.toString())) {
      throw new UnsupportedOperationException(column.aggregationType.toString() + " not supported "
          + "in fStream");
    }
    Aggregate aggregate = new Aggregate();
    FStreamAggregateType fStreamAggregateType = FStreamAggregateType.valueOf(column
        .aggregationType.toString());
    if (fStreamAggregateType.equals(FStreamAggregateType.DISTINCT_COUNT)) {
      aggregate.setAggregateType(new AggregateType(FSTREAM_UNIQUE_AGGREGATE_TYPE));
    }
    else {
      aggregate.setAggregateType(new AggregateType(fStreamAggregateType.toString()));
    }
    final String fstreamColumnName = COL_PREPEND + column.colName.split("\\.")[1];
    aggregate.setFieldName(fstreamColumnName);
    aggregateList.add(aggregate);
    fstreamColumnNames.add(fstreamColumnName);
  }

  @Override
  public void visitFrom(String fromTable) {
    factProperties = MetaAccessor.get().getTablePropertiesFromCache(fromTable);
    if (!factProperties.containsKey(FSTREAM_ID_KEY)) {
      throw new UnsupportedOperationException("Underlying fact is not fStream compatible");
    }
    id = factProperties.get(FSTREAM_ID_KEY);
    COL_PREPEND = factProperties.get(COL_PREPEND_KEY);
    GRANULARITY = factProperties.get(GRANULARITY_KEY);
  }

  @Override
  public void visitGroupBy(String groupByColumn) {
    groupByColumns.add(COL_PREPEND + groupByColumn.split("\\.")[1]);
  }

  @Override
  public void visitOrderBy(String orderByColumn, OrderByExp.Type type) {
    orderByColumns.add(COL_PREPEND + orderByColumn.split("\\.")[1]);
  }

  @Override
  public void visitDateRange(String column, Date start, Date end) {

    if (!isSingleDateRange) {
      isSingleDateRange = true;
    }
    else {
      throw new UnsupportedOperationException("Multiple date range columns are not supported");
    }
    range.setStartTime(start.getTime());
    range.setEndTime(end.getTime());
    range.setGranularity(new Granularity(GRANULARITY));
  }

  @Override
  public void visitDateHistogram(String alias, String columnName,
                                 Date from, Date to, long intervalMs,
                                 SelectColumn.DownSampleUnit downSampleUnit) {
    //A date histogram is a group by on time column. So add it to group by and order by as well.
    String onlyColName = columnName.split("\\.")[1];
    visitDateRange(onlyColName, from, to);
    visitGroupBy(columnName);
    visitOrderBy(columnName, null);
    final String fstreamColumnName = COL_PREPEND + onlyColName;
    simpleColumns.add(fstreamColumnName); //Adding it here so that it is compatible
    fstreamColumnNames.add(fstreamColumnName);
  }

  @Override
  public void visit(Criteria criteria) {
    final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
    criteria.accept(filterBuilder);

  }

  class RootCriteriaBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

    @Override public CriteriaVisitor visit(LogicalOp logicalOp) {

      if (!logicalOp.getType().equals(LogicalOp.Type.AND)) {
        throw new UnsupportedOperationException(
            "There are not handlers for this logical operator" + logicalOp.getType());
      }
      for (Criteria criteria : logicalOp.getCriteria()) {
        final RootCriteriaBuilder filterBuilder = new RootCriteriaBuilder();
        criteria.accept(filterBuilder);
      }
      return new DefaultCriteriaVisitor();
    }

    @Override
    public CriteriaVisitor visit(Param param) {
      try {
        Object value = param.getValue(paramValues);
        String operator = getOperatorForParam(param,paramValues);
        switch (operator){
          case "in":{
            inFilter.put(COL_PREPEND + param.name.split("\\.")[1], (List<Object>)value);
            break;
          }

          default: {
            if (value instanceof Collection<?>) {
              List<Object> arrayVal = (List<Object>) value;
              if (arrayVal.size() > 1) {
                throw new UnsupportedOperationException("Multiple param values are not supported in "
                        + "fstream");
              } else {
                value = arrayVal.get(0);
              }
            }
            if (param.name.contains("$") && !value.equals("eq")) {
              throw new UnsupportedOperationException(value + " is not supported in fstream");
            }
            filters.put(COL_PREPEND + param.name.split("\\.")[1], value);

          }
        }
      } catch (Exception e) {
        System.out.println("No param value provided");
      }
      return this;
    }

    private String getOperatorForParam(Param param, Map<String, String[]> paramValues) {

      String operatorKey = param.name + OPERATOR_KEY;

      if(paramValues.get(operatorKey) != null && paramValues.get(operatorKey).length > 0){
        return paramValues.get(operatorKey)[0];
      }
      return "";
    }
  }

  List<String> getOrderedFstreamColumns() {
    List<SelectColumn> columns = query.getSelectedColumns();
    return columns.stream()
            .map(column -> {
              if(column.getName().contains(".")){
                return column.getName().split("\\.")[1];
              }
              else{
                return column.getName();
              }
            })
            .filter(columnName -> fstreamColumnNames.contains(COL_PREPEND + columnName))
            .collect(Collectors.toList());
  }

  //This will return the FStreamRequest object and may throw exception in case some things violate
  @Override
  protected Object buildQueryImpl() {
    if (!groupByColumns.containsAll(simpleColumns)) {
      throw new UnsupportedOperationException("Non aggregate columns in select must be "
          + "present in group by");
    }
    if (!isAggregateColPresent) {
      throw new UnsupportedOperationException("Aggregate column is required for fstream");
    }
    FstreamRequest fstreamRequest = null;

    Map<String, Object> finalFilters = filters;
    Map<String,List<Object>> finalInFilter = inFilter;

    if(finalFilters.isEmpty()){
      finalFilters = null;
    }
    if(finalInFilter.isEmpty()){
      finalInFilter= null;
    }
    fstreamRequest = new FstreamRequest(groupByColumns, orderByColumns, finalFilters,
          range, aggregateList,finalInFilter);
    List<String> orderedFstreamColumns = getOrderedFstreamColumns();
    return new FStreamQuery(fstreamRequest, id, orderedFstreamColumns);
  }
}
