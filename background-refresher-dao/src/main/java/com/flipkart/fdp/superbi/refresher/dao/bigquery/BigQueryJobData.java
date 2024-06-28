package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.refresher.api.cache.JsonSerializable;
import lombok.Getter;

@Getter
public class BigQueryJobData implements JsonSerializable {

  private final String jobId;

  @JsonCreator
  public BigQueryJobData(@JsonProperty("jobId") String jobId) {
    this.jobId = jobId;
  }
}
