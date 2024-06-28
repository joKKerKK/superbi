package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 05/07/19
 */

public class ProvidedSourceExecutionContext extends ExecutionContext {
  private ProvidedSourceExecutionContext() {
    super();
  }

  private ProvidedSourceExecutionContext(
      Builder builder) {
    super(builder);
  }

  @Override
  protected void initSourceName() {
    // Ignore predicting it from query
    if(StringUtils.isBlank(sourceName)) {
      // sourceName was not provided by Builder
      throw new RuntimeException("No SourceName provided for query Execution");
    }
  }

  public static class Builder extends ExecutionContext.Builder<Builder> {
    public Builder() {
      super(new ProvidedSourceExecutionContext());
    }

    public Builder setSource(String sourceName) {
      instance.sourceName = sourceName;
      return this;
    }

  }
}
