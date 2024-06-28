package com.flipkart.fdp.superbi.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import lombok.NonNull;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "requestType",
    visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DSQueryRefreshRequest.class, name = "DSQuery"),
    @JsonSubTypes.Type(value = NativeQueryRefreshRequest.class, name = "NativeQuery"),
})
@AllArgsConstructor
public abstract class QueryRefreshRequest {
  @NonNull
  private String storeIdentifier;
  private String fromTable;
  private Map<String, String> appliedFilters;
}