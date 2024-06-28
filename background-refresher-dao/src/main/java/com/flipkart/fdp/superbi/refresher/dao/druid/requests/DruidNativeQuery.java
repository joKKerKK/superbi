package com.flipkart.fdp.superbi.refresher.dao.druid.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@AllArgsConstructor
public class DruidNativeQuery {

  @JsonProperty("query")
  String query;

  Map<String,Object> context;
}
