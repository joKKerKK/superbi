package com.flipkart.fdp.superbi.subscription.model.plato;

import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class PlatoMetaApiResponse<T> {
    private Boolean success;
    private String message;
    private Optional<T> data;
}
