package com.flipkart.fdp.superbi.subscription.model.plato;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class PlatoModelDataResponse {

    @JsonProperty("metadata")
    @Builder.Default
    protected Optional<QueryResultMetadata> responseMetadata = Optional.absent();

    @Builder.Default
    protected Optional<QueryResult> queryResult = Optional.absent();
}
