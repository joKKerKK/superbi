package com.flipkart.fdp.superbi.cosmos.dg;

import com.flipkart.fdp.superbi.cosmos.dg.exception.InternalErrorException;
import com.flipkart.fdp.superbi.cosmos.dg.ingestion.BatchHttpIngestor;
import com.flipkart.fdp.superbi.cosmos.dg.ingestion.BatchIngestor;
import com.flipkart.fdp.superbi.cosmos.dg.ingestion.Ingestor;
import com.flipkart.fdp.superbi.cosmos.dg.models.IngestibleBaseEntity;
import com.flipkart.fdp.superbi.cosmos.dg.models.IngestibleEntity;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.List;

public class IngestionFacade {
    private Function<IngestibleBaseEntity, IngestibleEntity> toEntity =
            IngestibleEntity::new;

    public String doIt(String uri, String entityName,
                     List<? extends IngestibleBaseEntity> baseEntities) throws InternalErrorException {
        Ingestor updater = new BatchIngestor(new BatchHttpIngestor(uri, entityName));
        List<IngestibleEntity> entity = Lists.transform(baseEntities, toEntity);
        return updater.ingest(entity);
    }
}