package com.flipkart.fdp.superbi.core.adaptor;

import com.flipkart.fdp.mmg.cosmos.api.TableEnhancementSTO;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.refresher.api.execution.MetaDataPayload;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Created by akshaya.sharma on 04/07/19
 */
@AllArgsConstructor
@Builder
@Getter
public class AdaptorQueryPayload {
  private final String storeIdentifier;
  private final String attemptKey;
  private final String cacheKey;
  private final long deadLine;
  // Allow native query to be passed from WEB
  // We intentionally force nativeQuery even if its a DSQuery execution. As only DSQuery execution is an antipattern in its current form
  private final NativeQuery nativeQuery;
  // Set dsQuery as null if only native query execution is needed
  private final DSQuery dsQuery;
  private final Map<String, String[]> params;
  private final Map<String, String> dateRange;
  private long queryWeight;
  private String reportAction;
  private String requestId;
  private MetaDataPayload metaDataPayload;
}
