package com.flipkart.fdp.superbi.dsl.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.Evaluable;
import com.flipkart.fdp.superbi.dsl.query.exp.ExprEvalException;
import com.flipkart.fdp.superbi.dsl.query.exp.OrderByExp;
import com.flipkart.fdp.superbi.dsl.query.exp.ParamExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.DSQueryVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.ParamVisitor;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * User: shashwat Date: 31/12/13
 * <p/>
 * A Query Q is defined as Q := Selection Criteria [ 'AND' DateRange] ; (* More components can be
 * added later - such as group by, Limit etc *) Selection := Column+ Criteria := (Predicate |
 * LogicalOp ); LogicalOp := LogicOperator (Criteria)+ ; LogicOperator := 'Not' | 'And' | 'Or' ; (*
 * represented as enum for documentation, actually they are independent classes *) Predicate :=
 * RelationalOperator (Exp)+ ; RelationalOperator := 'In' | 'Equals' | 'Range' | 'LessThan' |
 * 'GreaterThan' etc ; (* represented as enum for documentation, actually they are independent
 * classes *) Exp := (Arg|Fn) ([MathOp Exp+])+ ; Fn := FnName (Arg)+ ; (* A function that takes
 * arguments *) FnName := Identifier Arg := Column | Literal | Param Column := Identifier ; Param :=
 * Identifier ; Literal := Number | String | Date | etc ; Identifier := letter , ( letter | digit |
 * "_" )* DateRange := 'Range' Date [Date] (* a date-based filter of result *)
 * <p/>
 * <p/>
 * Definitions Selection => Set of selected columns Predicate => A filter condition in a query ; Op
 * => a Relational Operator; examples: In, Equals, Range, LessThan, GreaterThan etc ; Column => A
 * which contains the value specified by the user;
 */

// TODO: Ensure DSQuery is serializable through Representations

@Slf4j
public class DSQuery implements Serializable {

  private transient Optional<Criteria> criteria;
  private transient List<SelectColumn> selectedColumns; // ordered; if empty, select all; enable expressions here
  @Getter
  private transient List<DateRangePredicate> dateRangePredicate;
  private transient List<ColumnExp> groupByColumns;
  private transient List<OrderByExp> orderByColumns;
  private transient Optional<Integer> limit;
  private transient Optional<Integer> sample;
  private transient String fromTable;

  public boolean hasAggregations() {
    for (final SelectColumn column : selectedColumns) {
      if (column.type.equals(SelectColumn.Type.AGGREGATION)) {
        return true;
      }
    }
    return false;
  }


  private DSQuery() {
    this(
        Optional.<Criteria>absent(),
        ImmutableList.<SelectColumn>of(),
        Lists.<DateRangePredicate>newArrayList(),
        Lists.<ColumnExp>newArrayList(),
        Lists.<OrderByExp>newArrayList(),
        Optional.<Integer>absent(),
        Optional.<Integer>absent(),
        null
    );
  }

  public DSQuery(Optional<Criteria> criteria, List<SelectColumn> selectedColumns,
      List<DateRangePredicate> dateRangePredicate, List<ColumnExp> groupByColumns,
      List<OrderByExp> orderByColumns, Optional<Integer> limit, Optional<Integer> sample,
      String fromTable) {
    this.criteria = criteria;
    this.selectedColumns = selectedColumns;
    this.dateRangePredicate = dateRangePredicate;
    this.groupByColumns = groupByColumns;
    this.orderByColumns = orderByColumns;
    this.limit = limit;
    this.sample = sample;
    this.fromTable = fromTable;
  }

  public DSQuery(DSQuery dsQuery) {
    this(
        dsQuery.criteria,
        ImmutableList.copyOf(dsQuery.selectedColumns),
        dsQuery.dateRangePredicate,
        dsQuery.groupByColumns,
        dsQuery.orderByColumns,
        dsQuery.limit,
        dsQuery.sample,
        dsQuery.fromTable
    );
  }

  @JsonIgnore
  public Set<Param> getParams() {
    final ParamVisitor visitor = new ParamVisitor(new HashMap<String, String[]>());
    accept(visitor);
    return visitor.getParams();
  }

  @JsonIgnore
  public Schema getSchema(Map<String, String[]> params) {
    return new Schema(this, params, false);
  }

  @JsonIgnore
  public Schema getSchema(Map<String, String[]> params, boolean skipHiddenColumns) {
    return new Schema(this, params, skipHiddenColumns);
  }

  @JsonIgnore
  public List<DateRange> getDateRange(final Map<String, String[]> params) {
    final Evaluable.F.EvalFn<DateRange, DateRangePredicate> evalFn = Evaluable.F.evalFunc(params);
    final List<DateRange> dateRanges = Lists.newArrayList();
    for (DateRangePredicate predicate : dateRangePredicate) {
      try {
        dateRanges.add(predicate.evaluate(params));
      } catch (ParamMissingException pme) {
        log.warn("DateRange filter ignored: " + pme.getMessage());
      } catch (ExprEvalException e) {
        throw new RuntimeException(e);
      }
    }
    return dateRanges;
  }

  @JsonIgnore
  public Iterable<SelectColumn> getNonDerivedColumns() {
    return Iterables.filter(selectedColumns, Predicates.not(P.isDerived));
  }

  public List<SelectColumn> getSelectedColumns() {
    return selectedColumns;
  }

  public List<ColumnExp> getGroupByColumns() {
    return groupByColumns;
  }

  public List<OrderByExp> getOrderByColumns() {
    return orderByColumns;
  }

  @JsonIgnore
  public List<SelectColumn> getVisibleSelectedColumns() {
    return selectedColumns.stream().filter(col -> col.isVisible()).collect(
        Collectors.toList());
  }

  @JsonIgnore
  public Iterable<SelectColumn> getDerivedColumns() {
    return Iterables.filter(selectedColumns, P.isDerived);
  }

  public Optional<Criteria> getCriteria() {
    return criteria;
  }

  public Optional<Integer> getLimit() {
    return limit;
  }


  public String getFromTable() {
    return fromTable;
  }

  @JsonIgnore
  public Set<Param> getDateParams() {
    //todo: may need visitor here
    Set<Param> dateParams = Sets.newHashSet();
    for (DateRangePredicate dateRangePred : dateRangePredicate) {
      if (dateRangePred.start instanceof ParamExp) {
        Param param = ((ParamExp) dateRangePred.start).param;
        dateParams.add(param);
      }
      if (dateRangePred.end instanceof ParamExp) {
        Param param = ((ParamExp) dateRangePred.end).param;
        dateParams.add(param);
      }
    }
    return dateParams;
  }

  @JsonIgnore
  public Optional<SelectColumn.DateHistogram> getDateHistogramCol() {
    for (SelectColumn col : getSelectedColumns()) {
      if (col instanceof SelectColumn.DateHistogram) {
        return Optional.of((SelectColumn.DateHistogram) col);
      }
    }
    return Optional.absent();
  }

  public void accept(final DSQueryVisitor visitor) {

    visitor.visitFrom(fromTable);
    Map<String, String[]> paramValues = visitor.getParamValues();

    for (SelectColumn col : selectedColumns) {
      col.accept(visitor);
    }

    if (criteria.isPresent()) {
      visitor.visit(criteria.get());
    }

    /*HACK TEMPORARY*/
    if (paramValues.isEmpty()) {
      for (DateRangePredicate predicate : dateRangePredicate) {
        visitor.visit(predicate);
      }
    } else {
      for (DateRange dateRange : getDateRange(paramValues)) {
        visitor.visitDateRange(dateRange.getColumn(), dateRange.getStart(), dateRange.getEnd());
      }
    }

    for (ColumnExp groupBy : groupByColumns) {
      visitor.visitGroupBy(groupBy.evaluateAndGetColName(paramValues));
    }

    for (OrderByExp orderBy : orderByColumns) {
      try {
        visitor.visitOrderBy(
            orderBy.exp.evaluateAndGetColName(paramValues),
            OrderByExp.Type.valueOf(orderBy.evalueateAndGetType(paramValues))
        );
      } catch (ParamMissingException ignored) {
        log.warn("Order by is ignored: " + ignored.getMessage());
      }
    }

    visitor.visit(limit);

    if (sample.isPresent()) {
      visitor.visitSample(sample.get());
    }

  }

    /*public void accept(final DSQueryVisitor visitor, Map<String, String[]> params) {

        visitor.visitFrom(fromTable);

        for(SelectColumn col: selectedColumns) {
            col.accept(visitor);
        }

        if (criteria.isPresent()) {
            visitor.visit(criteria.get());
        }

        for(DateRange dateRange : getDateRange(params)) {
            if(dateRange.getIntervalMs().isPresent()) {
                visitor.visitDateHistogram(
                            dateRange.getColumn(),
                            dateRange.getColumn(),
                            dateRange.getStart(),
                            dateRange.getEnd(),
                            dateRange.getIntervalMs().get()
                        );
            } else {
                visitor.visitDateRange(dateRange.getColumn(), dateRange.getStart(), dateRange.getEnd());
            }
        }

        for(ColumnExp groupBy : groupByColumns) {
            visitor.visitGroupBy(groupBy.evaluateAndGetColName(params));
        }

        for(OrderByExp orderBy : orderByColumns) {
            visitor.visitOrderBy(
                    orderBy.exp.evaluateAndGetColName(params),
                    OrderByExp.Type.valueOf(orderBy.evalueateAndGetType(params))
            );
        }

        if (limit.isPresent())
            visitor.visit(limit.get());
    }*/

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(DSQuery another) {
    return new Builder(another);
  }

  public boolean hasHistograms() {
    for (SelectColumn column : selectedColumns) {
      if (column instanceof SelectColumn.DateHistogram ||
          column instanceof SelectColumn.Histogram) {
        return true;
      }
    }
    return false;
  }

  public boolean hasGroupBys() {
    return groupByColumns.size() > 0;
  }

  public static class Builder {

    private final DSQuery dsQuery;

    public Builder(DSQuery another) {
      dsQuery = new DSQuery(another);
    }

    public Builder() {
      dsQuery = new DSQuery();
    }

    public Builder withCriteria(Criteria criteria) {
      dsQuery.criteria = Optional.fromNullable(criteria);
      return this;
    }

    public Builder withDataRange(DateRangePredicate... predicate) {
      dsQuery.dateRangePredicate = Lists.newArrayList(predicate);
      return this;
    }

    public Builder withColumns(List<SelectColumn> columns) {
      dsQuery.selectedColumns = columns;
      return this;
    }

    public Builder withGroupByColumns(List<String> groupByColumns) {
      dsQuery.groupByColumns = Lists.transform(groupByColumns, ColumnExp.fromColName);
      return this;
    }

    public Builder withOrderByColumns(List<String> orderByColumns) {
      dsQuery.orderByColumns = Lists.transform(orderByColumns, OrderByExp.fromColName);
      return this;
    }

    public Builder withGroupByColumnExps(List<ColumnExp> groupByColumns) {
      dsQuery.groupByColumns = groupByColumns;
      return this;
    }

    public Builder withOrderByColumnExps(List<OrderByExp> orderByColumns) {
      dsQuery.orderByColumns = orderByColumns;
      return this;
    }

    public Builder withLimit(Integer limit) {
      dsQuery.limit = Optional.of(limit);
      return this;
    }

    public Builder withSample(Integer sample) {
      dsQuery.sample = Optional.of(sample);
      return this;
    }

    public Builder withFrom(String fromTable) {
      dsQuery.fromTable = fromTable;
      return this;
    }

    public DSQuery build() {
      validate(dsQuery);
      return new DSQuery(dsQuery);
    }

    private void validate(DSQuery dsQuery) {
      Preconditions.checkArgument(!(dsQuery.fromTable == null), "From table is mandatory");
    }
  }

  public static class P {

    public static Predicate<? super SelectColumn> isDerived = new Predicate<SelectColumn>() {
      @Override
      public boolean apply(@Nullable SelectColumn column) {
        return column.type.equals(SelectColumn.Type.DERIVED);
      }
    };
  }

  @Override
  public String toString() {
    return "DSQuery{" +
        "criteria=" + criteria +
        ", selectedColumns=" + selectedColumns +
        ", dateRangePredicate=" + dateRangePredicate +
        ", groupByColumns=" + groupByColumns +
        ", orderByColumns=" + orderByColumns +
        ", limit=" + limit +
        ", sample=" + sample +
        ", fromTable='" + fromTable + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DSQuery)) {
      return false;
    }
    DSQuery dsQuery = (DSQuery) o;
    return Objects.equal(getCriteria(), dsQuery.getCriteria()) &&
        Objects.equal(getSelectedColumns(), dsQuery.getSelectedColumns()) &&
        Objects.equal(dateRangePredicate, dsQuery.dateRangePredicate) &&
        Objects.equal(getGroupByColumns(), dsQuery.getGroupByColumns()) &&
        Objects.equal(getOrderByColumns(), dsQuery.getOrderByColumns()) &&
        Objects.equal(getLimit(), dsQuery.getLimit()) &&
        Objects.equal(sample, dsQuery.sample) &&
        Objects.equal(getFromTable(), dsQuery.getFromTable());
  }
}
