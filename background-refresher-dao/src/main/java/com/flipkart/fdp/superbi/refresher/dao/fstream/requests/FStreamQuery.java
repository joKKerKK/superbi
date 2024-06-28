package com.flipkart.fdp.superbi.refresher.dao.fstream.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

@Getter
@EqualsAndHashCode
public class FStreamQuery {
    private final FstreamRequest fstreamRequest;
    private final String fstreamId;
    private final List<String> orderedFstreamColumns;

    @JsonCreator
    public FStreamQuery(
        @JsonProperty("fstreamRequest") FstreamRequest fstreamRequest,
        @JsonProperty("fstreamId") String fstreamId,
        @JsonProperty("orderedFstreamColumns") List<String> orderedFstreamColumns) {
        this.fstreamRequest = fstreamRequest;
        this.fstreamId = fstreamId;
        this.orderedFstreamColumns = orderedFstreamColumns;
    }
}
