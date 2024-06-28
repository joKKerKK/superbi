package com.flipkart.fdp.superbi.refresher.dao.druid.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@EqualsAndHashCode
public class DruidQuery {

  @JsonProperty("query")
  String query;

  private List<String> headerList;

  Map<String,Object> context;


  @JsonCreator
  public DruidQuery(@JsonProperty("query") String query,
      @JsonProperty("headerList") List<String> headerList,
      @JsonProperty("context") Map<String,Object> context) {
    this.query = query;
    this.headerList = headerList;
    this.context = context;
  }
}
