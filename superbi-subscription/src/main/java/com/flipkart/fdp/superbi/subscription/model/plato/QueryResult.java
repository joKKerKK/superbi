package com.flipkart.fdp.superbi.subscription.model.plato;

import lombok.*;

import java.util.List;

@Getter
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {

    @NonNull
    private List<SchemaInfo> schema;
    @NonNull
    private List<List<Object>> results;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaInfo {

        private String columnAlias;
        private DataType columnType;

        private enum DataType {
            NUMBER, STRING, TIMESTAMP, BOOLEAN
        }
    }
}
