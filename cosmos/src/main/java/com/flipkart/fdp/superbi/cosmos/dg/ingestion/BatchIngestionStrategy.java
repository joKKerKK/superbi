package com.flipkart.fdp.superbi.cosmos.dg.ingestion;

import com.flipkart.fdp.superbi.cosmos.dg.models.IngestibleEntity;
import java.util.List;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public interface BatchIngestionStrategy {
    public void init();
    public void ingest(IngestibleEntity entity);
    public void ingest(List<IngestibleEntity> entities);
    public String commit();
}
