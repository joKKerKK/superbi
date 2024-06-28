package com.flipkart.fdp.superbi.cosmos.data.api.execution.druid;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Getter;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class DruidQuery {

  public static final String SQL_TIME_ZONE_KEY = "sqlTimeZone";

  @JsonProperty("query")
  String query;

  private List<String> headerList;

  Map<String, Object> context;


  public DruidQuery(String query, List<String> headerList, Map<String, Object> context) {
    this.query = query;
    this.headerList = headerList;
    this.context = context;
  }
}
