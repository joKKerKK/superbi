package com.flipkart.fdp.superbi.cosmos.dg.models;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public interface IngestibleBaseEntity {
    public String getSchemaVersion();
    public String getEntityId();
    public String getUpdatedAt();
}
