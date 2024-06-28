package com.flipkart.fdp.superbi.cosmos.data.api.execution.druid;

import com.flipkart.fdp.superbi.dsl.query.*;
import com.flipkart.fdp.superbi.dsl.query.exp.ColumnExp;
import com.flipkart.fdp.superbi.dsl.query.exp.EvalExp;
import com.flipkart.fdp.superbi.dsl.query.visitors.CriteriaVisitor;
import com.flipkart.fdp.superbi.dsl.query.visitors.impl.DefaultCriteriaVisitor;
import com.flipkart.fdp.superbi.exceptions.InvalidQueryException;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Getter
@Slf4j
public class EuclidQueryBuilder extends DruidQueryBuilder {
    private final List<String> filteredColumns = Lists.newArrayList();
    private final List<String> selectDimensions = Lists.newArrayList();
    private final List<String> selectMetrics = Lists.newArrayList();
    private final List<String> groupByFields = Lists.newArrayList();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final List<EuclidRuleDefinition> euclidRules;
    private final Map<String, String[]> paramValues;
    private Date startDate;
    private Date endDate;
    private int dimensionCount = 0;
    private int metricCount = 0;
    private boolean isLovRequest = false;

    @SneakyThrows
    public EuclidQueryBuilder(DSQuery query, Map<String, String[]> values, DruidDSLConfig config, List<String> euclidRulesList) {
        super(query, values, config);
        paramValues = values;
        euclidRules = new ArrayList<>();
        if (euclidRulesList != null) {
            for (String jsonString : euclidRulesList) {
                EuclidRuleDefinition euclidRule = JsonUtil.fromJson(jsonString, EuclidRuleDefinition.class);
                euclidRules.add(euclidRule);
            }
        }
    }


    @Override
    public void visit(SelectColumn.SimpleColumn column) {
        if (!column.isNativeExpression) {
            selectDimensions.add(
                    getModifiedColumn(column.colName));
        }
        super.visit(column);
    }

    @Override
    public void visit(SelectColumn.Aggregation column,
                      SelectColumn.AggregationOptions options) {
        if (!column.isNativeExpression) {
            selectMetrics.add(getModifiedColumn(column.colName));
        }
        super.visit(column, options);
    }

    @Override
    public void visit(SelectColumn.ConditionalAggregation column) {
        if (!column.isNativeExpression) {
            selectMetrics.add(getModifiedColumn(column.colName));
        }
        super.visit(column);
    }

    @Override
    @SneakyThrows
    public void visit(DateRangePredicate dateRangePredicate) {
        DateRange range = dateRangePredicate.evaluate(paramValues);
        if (range != null) {
            String columnName = getModifiedColumn(range.getColumn());
            Date from = range.getStart();
            Date to = range.getEnd();
            validateTimeRules(columnName, from, to);
        }
        super.visit(dateRangePredicate);
    }


    @Override
    public void visitDateRange(String column, Date start, Date end) {
        validateTimeRules(column, start, end);
        super.visitDateRange(column, start, end);

    }

    @Override
    public void visitHistogram(String alias, String columnName, long from, long to, long interval) {
        validateTimeRules(columnName, new Date(from), new Date(to));
        super.visitHistogram(alias, columnName, from, to, interval);
    }


    @Override
    public void visitGroupBy(String groupByColumn) {
        groupByFields.add(getModifiedColumn(groupByColumn));
        for (String dimension : filteredColumns) {
            groupByFields.remove(dimension);
        }
        long daysDifference = calculateDaysDifference(startDate, endDate);
        if (euclidRules != null) {
            for (EuclidRuleDefinition euclidRule : euclidRules) {
                if (!groupByFields.isEmpty()) {
                    List<GroupByRule> groupByRules = euclidRule.getGroupByRule();
                    if (groupByRules != null) {
                        int groupByCount = groupByFields.size();
                        for (GroupByRule groupByRule : groupByRules) {
                            String ruleType = groupByRule.getType();
                            if (ruleType.equals("dimensionWithLimit") && groupByRule.getApplicableDimensions() != null)
                                if (groupByFields.containsAll(groupByRule.getApplicableDimensions()) && groupByCount > groupByRule.getLimit()) {
                                    throw new InvalidQueryException(groupByRule.getErrorMessage());
                                }
                            if (ruleType.equals("dimensionWithLimitAndTime") && groupByRule.getApplicableDimensions() != null) {
                                if (groupByFields.containsAll(groupByRule.getApplicableDimensions()) && groupByCount > groupByRule.getLimit()) {
                                    TimeRangeRule timeRangeRule = groupByRule.getTimeRangeRule();
                                    if (timeRangeRule != null && timeRangeRule.getGranularity().equals("day") && daysDifference > timeRangeRule.getLimit()) {
                                        throw new InvalidQueryException(groupByRule.getErrorMessage());
                                    }
                                }
                            }
                            if (ruleType.equals("dimensionWithTime") && groupByRule.getApplicableDimensions() != null) {
                                if (groupByFields.containsAll(groupByRule.getApplicableDimensions())) {
                                    TimeRangeRule timeRangeRule = groupByRule.getTimeRangeRule();
                                    if (timeRangeRule != null && timeRangeRule.getGranularity().equals("day") && daysDifference > timeRangeRule.getLimit()) {
                                        throw new InvalidQueryException(groupByRule.getErrorMessage());
                                    }
                                }
                            }
                            if (ruleType.equals("dimensionCountWithTime") && groupByRule.getApplicableDimensions() == null) {
                                if (groupByCount > groupByRule.getLimit()) {
                                    TimeRangeRule timeRangeRule = groupByRule.getTimeRangeRule();
                                    if (timeRangeRule != null && timeRangeRule.getGranularity().equals("day") && daysDifference > timeRangeRule.getLimit()) {
                                        throw new InvalidQueryException(groupByRule.getErrorMessage());
                                    }
                                }
                            }
                            if (ruleType.equals("failDimensionCombination") && groupByRule.getApplicableDimensions() != null) {
                                if (groupByFields.containsAll(groupByRule.getApplicableDimensions())) {
                                    throw new InvalidQueryException(groupByRule.getErrorMessage());
                                }
                            }
                        }
                    }
                }
            }
        }
        super.visitGroupBy(groupByColumn);
    }

    @Override
    public void visitDateHistogram(String alias, String columnName,
                                   Date from, Date to, long intervalMs,
                                   SelectColumn.DownSampleUnit downSampleUnit) {
        validateTimeRules(columnName, from, to);
        long daysDifference = calculateDaysDifference(from, to);
        if (euclidRules != null) {
            for (EuclidRuleDefinition euclidRule : euclidRules) {
                List<GroupByRule> groupByRules = euclidRule.getGroupByRule();
                if (groupByRules != null) {
                    for (GroupByRule groupByRule : groupByRules) {
                        if (groupByRule.getType().equals("timeGranularity")) {
                            TimeRangeRule timeRangeRule = groupByRule.getTimeRangeRule();
                            if (timeRangeRule != null && (downSampleUnit.equals(SelectColumn.DownSampleUnit.Hours) || intervalMs<86400000) && daysDifference > timeRangeRule.getLimit()) {
                                throw new InvalidQueryException(groupByRule.getErrorMessage());
                            }
                        }
                    }
                }
            }
        }
        super.visitDateHistogram(alias, columnName, from, to, intervalMs, downSampleUnit);
    }

    private void validateTimeRules(String columnName, Date from, Date to) {
        if (euclidRules != null) {
            for (EuclidRuleDefinition euclidRule : euclidRules) {
                StartTimeRule startTimeRule = euclidRule.getStartTimeRule();
                TimeRangeRule timeRangeRule = euclidRule.getTimeRangeRule();

                if (startTimeRule != null && startTimeRule.getColumnName() != null && isMatchingColumn(columnName, startTimeRule.getColumnName())) {
                    handleStartTimeRule(from, to, startTimeRule);
                }

                if (timeRangeRule != null && timeRangeRule.getColumnName() != null && isMatchingColumn(columnName, timeRangeRule.getColumnName())) {
                    handleTimeRangeRule(from, to, timeRangeRule);
                }
            }
        }
    }

    private boolean isMatchingColumn(String columnName, String ruleColumnName) {
        return getModifiedColumn(columnName).equals(ruleColumnName);
    }

    private void handleStartTimeRule(Date from, Date to, StartTimeRule startTimeRule) {

        if (from != null && to != null) {
            startDate = from;
            endDate = to;
        }

        if (from.after(to)) {
            throw new InvalidQueryException(startTimeRule.getErrorMessage());
        }

        if ("day".equals(startTimeRule.getGranularity())) {
            Date earliestStartDate = new Date((long) (System.currentTimeMillis() - (startTimeRule.getLimit() * 24L * 60L * 60L * 1000L)));
            if (from.before(earliestStartDate)) {
                throw new InvalidQueryException(startTimeRule.getErrorMessage());
            }
        }
    }

    private void handleTimeRangeRule(Date from, Date to, TimeRangeRule timeRangeRule) {
        if (from != null && to != null) {
            startDate = from;
            endDate = to;
        }
        long daysDifference = calculateDaysDifference(from, to);

        if ("day".equals(timeRangeRule.getGranularity()) && daysDifference > timeRangeRule.getLimit()) {
            throw new InvalidQueryException(timeRangeRule.getErrorMessage());
        }
    }

    private String getModifiedColumn(String columnName) {
        return columnName.split("\\.").length == 1 ? columnName : columnName.split("\\.")[1];
    }

    @Override
    protected Object buildQueryImpl() {
        long daysDifference = calculateDaysDifference(startDate, endDate);

        if (euclidRules != null) {
            for (EuclidRuleDefinition euclidRule : euclidRules) {
                List<SelectRule> selectRules = euclidRule.getSelectRule();
                List<GroupByRule> groupByRules = euclidRule.getGroupByRule();
                if (selectRules != null) {

                    for (SelectRule selectRule : selectRules) {
                        handleSelectRule(selectRule, daysDifference);
                    }
                }
                isLovRequest = (
                        (selectMetrics.size() == 0)
                                && (selectDimensions.size() == 1)
                                && (groupByFields.size() == 1)
                                && selectDimensions.equals(groupByFields)
                );
                if (groupByRules != null && !isLovRequest) {
                    for (GroupByRule groupByRule : groupByRules) {
                        if (groupByRule.getType().equals("failDimensionWithoutFilter") && groupByRule.getApplicableDimensions() != null) {
                            for (String column : groupByFields) {
                                if (groupByRule.getApplicableDimensions().contains(column) && !filteredColumns.contains(column)) {
                                    if (groupByRule.getExceptionDimensionsList() != null && !filteredColumns.stream().anyMatch(groupByRule.getExceptionDimensionsList()::contains)) {
                                        throw new InvalidQueryException(groupByRule.getErrorMessage());
                                    } else if (groupByRule.getExceptionDimensionsList() == null) {
                                        throw new InvalidQueryException(groupByRule.getErrorMessage());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return super.buildQueryImpl();
    }

    private long calculateDaysDifference(Date startDate, Date endDate) {
        long millisecondsPerDay = 24L * 60L * 60L * 1000L;
        if (endDate != null && startDate != null) {
            return (endDate.getTime() - startDate.getTime()) / millisecondsPerDay;
        }
        return 0;
    }

    @Override
    public void visit(Criteria criteria) {
        final EuclidQueryBuilder.RootCriteriaBuilder filterBuilder = new EuclidQueryBuilder.RootCriteriaBuilder();
        criteria.accept(filterBuilder);
        super.visit(criteria);
    }

    private void handleSelectRule(SelectRule selectRule, long daysDifference) {
        String ruleType = selectRule.getType();
        metricCount = selectMetrics.size();
        dimensionCount = selectDimensions.size();

        if (("selectDimensionWithLimit").equals(ruleType) && dimensionCount > selectRule.getLimit()) {
            throw new InvalidQueryException(selectRule.getErrorMessage());
        }
        if (("selectMetricWithLimit").equals(ruleType) && metricCount > selectRule.getLimit()) {
            throw new InvalidQueryException(selectRule.getErrorMessage());
        }
        if (("metricWithLimit").equals(ruleType) && selectRule.getApplicableMetrics() != null) {
            if (selectMetrics.containsAll(selectRule.getApplicableMetrics()) && metricCount > selectRule.getLimit()) {
                throw new InvalidQueryException(selectRule.getErrorMessage());
            }
        }

        if ("metricCountWithTime".equals(ruleType) && selectRule.getApplicableMetrics() == null) {
            if (metricCount > selectRule.getLimit() && selectRule.getTimeRangeRule() != null) {
                TimeRangeRule timeRangeRule = selectRule.getTimeRangeRule();
                if (timeRangeRule.getGranularity().equals("day") && daysDifference > timeRangeRule.getLimit()) {
                    throw new InvalidQueryException(selectRule.getErrorMessage());
                }
            }
        }

        if ("metricWithLimitAndTime".equals(ruleType) && selectRule.getApplicableMetrics() != null) {
            if (selectMetrics.containsAll(selectRule.getApplicableMetrics()) && metricCount > selectRule.getLimit()) {
                TimeRangeRule timeRangeRule = selectRule.getTimeRangeRule();
                if (timeRangeRule != null && timeRangeRule.getGranularity().equals("day") && daysDifference > timeRangeRule.getLimit()) {
                    throw new InvalidQueryException(selectRule.getErrorMessage());
                }
            }
        }
        if ("metricWithTime".equals(ruleType) && selectRule.getApplicableMetrics() != null) {
            if (selectMetrics.containsAll(selectRule.getApplicableMetrics())) {
                TimeRangeRule timeRangeRule = selectRule.getTimeRangeRule();
                if (timeRangeRule != null && timeRangeRule.getGranularity().equals("day") && daysDifference > timeRangeRule.getLimit()) {
                    throw new InvalidQueryException(selectRule.getErrorMessage());
                }
            }
        }

        if ("defaultLimit".equals(ruleType) && (metricCount + dimensionCount) > selectRule.getLimit()) {
            throw new InvalidQueryException(selectRule.getErrorMessage());
        }
    }

    class RootCriteriaBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

        @Override
        public CriteriaVisitor visit(Predicate predicate) {
            final PredicateNodeBuilder localBuilder = new PredicateNodeBuilder(predicate);
            predicate.accept(localBuilder);
            return new DefaultCriteriaVisitor();
        }
    }

    class PredicateNodeBuilder extends DefaultCriteriaVisitor implements CriteriaVisitor {

        private final Predicate predicate;
        private String columnName;

        public PredicateNodeBuilder(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public CriteriaVisitor visit(Exp expression) {

            if (expression instanceof ColumnExp && columnName == null) {
                columnName = getModifiedColumn(((ColumnExp) expression).evaluateAndGetColName(paramValues));
                filteredColumns.add(columnName);
            }
            return this;
        }

        @Override
        public CriteriaVisitor visit(EvalExp expression) {
            return this;
        }

        @Override
        public CriteriaVisitor visit(Param param) {
            return this;
        }
    }
}
