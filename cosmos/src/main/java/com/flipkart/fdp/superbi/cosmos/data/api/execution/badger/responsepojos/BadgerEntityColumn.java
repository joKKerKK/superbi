package com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BadgerEntityColumn {
    @JsonProperty
    private String name;

    @JsonProperty
    private String type;

    @JsonProperty
    private int maxLength;

    @JsonProperty
    private boolean primaryKey;

    @JsonProperty("partition")
    private boolean partitioned;
}
