package com.flipkart.fdp.superbi.cosmos.data.api.execution.druid;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class EuclidRuleDefinition {
    private StartTimeRule startTimeRule;
    private TimeRangeRule timeRangeRule;
    private List<GroupByRule> groupByRule;
    private List<SelectRule> selectRule;
}

@Data
class StartTimeRule {
    private String granularity;
    private double limit;
    private String columnName;
    private String errorMessage;
}
@Data
class TimeRangeRule {
    private String granularity;
    private double limit;
    private String columnName;
    private String errorMessage;
}
@Data
class GroupByRule {
    private String type;
    private double limit;
    private String timeGranularity;
    private TimeRangeRule timeRangeRule;
    private List<String> applicableDimensions;
    private List<String> exceptionDimensionsList;
    private String errorMessage;

}
@Data
class SelectRule {
    private String type;
    private double limit;
    private TimeRangeRule timeRangeRule;
    private List<String> applicableMetrics;
    private String errorMessage;
}
