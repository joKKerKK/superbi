package com.flipkart.fdp.superbi.refresher.api.result.query;

import com.flipkart.fdp.superbi.dsl.query.Schema;
import lombok.Builder;
import lombok.Getter;

@Getter
public class RawQueryResultWithSchema extends RawQueryResult {

  private final Schema schema;

  @Builder(builderMethodName = "withSchemaBuilder")
  public RawQueryResultWithSchema(Schema schema,
      RawQueryResult rawQueryResult) {
    super(rawQueryResult.getData(), rawQueryResult.getColumns(), rawQueryResult.getDateHistogramMeta());
    this.schema = schema;
  }
}
