package com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;


@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BadgerProcessData {
    @JsonProperty
    private int id;

    @JsonProperty
    private String type;

    @JsonProperty
    private String org;

    @JsonProperty
    private String ns;

    @JsonProperty
    private String namespace;

    @JsonProperty
    private String name;

    @JsonProperty
    private String processString;

    @JsonProperty
    private String sideLineQuery;

    @JsonProperty
    private String mergeQuery;

    @JsonProperty
    private List<Integer> dependentIds;

    @JsonProperty
    private List<Integer> optionalDependentIds;

    @JsonProperty
    private String state;

    @JsonProperty
    private String assignedQueue="";

    @JsonProperty
    private List<BadgerEntity> entities;

}
