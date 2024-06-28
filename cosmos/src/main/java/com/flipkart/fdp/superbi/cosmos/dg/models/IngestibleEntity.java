package com.flipkart.fdp.superbi.cosmos.dg.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
@Getter
public final class IngestibleEntity {
    @JsonProperty
    private String schemaVersion;
    @JsonProperty
    private String entityId;
    @JsonProperty
    private String updatedAt;

    @JsonProperty("data")
    private IngestibleBaseEntity entityBody;

    public IngestibleEntity(IngestibleBaseEntity entityBody) {
        this.entityBody = entityBody;
        this.schemaVersion = entityBody.getSchemaVersion();
        this.entityId = entityBody.getEntityId();
        this.updatedAt = entityBody.getUpdatedAt();
    }
}
