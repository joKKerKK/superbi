package com.flipkart.fdp.superbi.core.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.dsl.DataType;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Created by akshaya.sharma on 09/07/19
 */
@AllArgsConstructor
@Builder
public class ReportFilters {
  private final List<FilterGroup> dateRange = Lists.newArrayList();
  private final List<FilterGroup> histogram = Lists.newArrayList();
  private  final List<FilterGroup> normal = Lists.newArrayList();
  private  final List<FilterGroup> cascading = Lists.newArrayList();
  private final GroupByFilterGroup groupBy;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private static class FilterGroup
  {
    private List<Object> valuesForSeries;

    @JsonProperty("alias")
    private String groupName;

    private List<QueryParam> params = Lists.newArrayList();

    public  void add(QueryParam param)
    {
      params.add(param);
    }
    public  void addAll(List<QueryParam> params)
    {
      this.params.addAll(params);
    }
  }

  @AllArgsConstructor
  @Data
  private static class QueryParam
  {
    private final String displayName;
    private final String paramName;
    private final String fetchURI;
    private final DataType dataType;
    private final List<ReportAssoication> mappings;
  }

  @AllArgsConstructor
  @Data
  public static class GroupByFilterGroup
  {
    private final List<FilterGroup> normal;
    private final List<FilterGroup> cascading;
  }

  @AllArgsConstructor
  @Data
  private static class ReportAssoication
  {
    public final String reportPath;
    public final String paramName;
  }
}
