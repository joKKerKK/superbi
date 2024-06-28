package com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BadgerEntity {
    @JsonProperty
    private long id;

    @JsonProperty
    private String name;

    @JsonProperty
    private String description;

    @JsonProperty
    private String tags;

    @JsonProperty
    private List<BadgerEntityColumn> columns;

    @JsonProperty
    private BadgerEntityStore entityStore;

    @JsonProperty
    private String schedule;

    @JsonProperty
    private boolean toSchedule;

    @JsonProperty("incr_mode")
    private Table.IncrMode incrMode = Table.IncrMode.COMPLETE;
}
