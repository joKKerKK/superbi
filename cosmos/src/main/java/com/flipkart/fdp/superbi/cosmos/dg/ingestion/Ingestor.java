package com.flipkart.fdp.superbi.cosmos.dg.ingestion;

import com.flipkart.fdp.superbi.cosmos.dg.exception.InternalErrorException;
import com.flipkart.fdp.superbi.cosmos.dg.models.IngestibleEntity;
import java.util.List;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public interface Ingestor {
    String ingest(List<IngestibleEntity> entities) throws InternalErrorException;
}
