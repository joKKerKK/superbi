package com.flipkart.fdp.superbi.subscription.model.plato;

import com.google.common.base.Optional;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@Getter
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class QueryResultMetadata {

    @NonNull
    protected String cacheKey;
    @NonNull
    protected Long freshAsOf;
    private Optional<Integer> totalNumberOfRows = Optional.absent();
    private Optional<Integer> truncatedRows = Optional.absent();
    @Builder.Default
    private Boolean truncated = false;
    @Builder.Default
    private Optional<String> d42Link = Optional.absent();
}
