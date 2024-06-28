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
public class BadgerEntityStore {

    @JsonProperty
    private String type;

    @JsonProperty
    private long created;

    @JsonProperty
    private long updated;

    @JsonProperty
    private String location;

    @JsonProperty
    private boolean reportReady;
}
