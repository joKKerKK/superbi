package com.flipkart.fdp.superbi.gcs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;


@Getter
public class GcsConfig {
    private final String projectId;
    private final String bucket;
    private final ArrayList<String> allowedDatastores;

    @JsonCreator
    @Builder
    public GcsConfig(@JsonProperty("projectId") String projectId,
                     @JsonProperty("bucket") String bucket,
                     @JsonProperty("allowedDatastores") ArrayList<String> allowedDatastores) {
        this.bucket = bucket;
        this.projectId = projectId;
        this.allowedDatastores = allowedDatastores;
    }
}
