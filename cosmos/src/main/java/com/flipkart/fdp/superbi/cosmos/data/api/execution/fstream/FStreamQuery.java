package com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream;

import com.flipkart.fdp.superbi.cosmos.data.api.execution.fstream.requestpojos.FstreamRequest;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class FStreamQuery {
  private FstreamRequest fstreamRequest;
  private String fstreamId;
  private List<String> orderedFstreamColumns;

  public FStreamQuery(FstreamRequest fstreamRequest, String fstreamId) {
    this(fstreamRequest, fstreamId, Lists.newArrayList());
  }

  public FStreamQuery(FstreamRequest fstreamRequest, String fstreamId, List<String> orderedFstreamColumns) {
    this.fstreamRequest = fstreamRequest;
    this.fstreamId = fstreamId;
    this.orderedFstreamColumns = orderedFstreamColumns;
  }

}
