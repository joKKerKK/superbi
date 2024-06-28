package com.flipkart.fdp.superbi.cosmos.dg.ingestion;

import com.flipkart.fdp.superbi.cosmos.dg.exception.InternalErrorException;
import com.flipkart.fdp.superbi.cosmos.dg.models.IngestibleEntity;
import java.util.List;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public class BatchIngestor implements Ingestor {
    private BatchIngestionStrategy dartIngestor;

    public BatchIngestor(BatchIngestionStrategy batchIngestionStrategy) {
        this.dartIngestor = batchIngestionStrategy;
    }

    @Override
    public String ingest(List<IngestibleEntity> entities) throws InternalErrorException {
        try {
            dartIngestor.init();
            dartIngestor.ingest(entities);
            return dartIngestor.commit();
        } catch (InternalErrorException ex) {
            throw ex;
        }
    }
}
