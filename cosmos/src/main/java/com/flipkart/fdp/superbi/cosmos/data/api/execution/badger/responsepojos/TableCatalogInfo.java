package com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
@NoArgsConstructor
public class TableCatalogInfo {
    public String tableName;
    public String databaseName;
    public String metaStoreType;
    public String tableType;
    public String version;
    public String status;
}
