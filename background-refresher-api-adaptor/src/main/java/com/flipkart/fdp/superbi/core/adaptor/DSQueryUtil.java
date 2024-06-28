package com.flipkart.fdp.superbi.core.adaptor;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.text.MessageFormat;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DSQueryUtil {

  public static QueryPayload checkDSQuerySerialization(QueryPayload queryPayload) {
    DSQuery dsQuery = queryPayload.getDsQuery();
    String jsonString = JsonUtil.toJson(dsQuery);
    try {
      DSQuery deserializedDSQuery = JsonUtil.fromJson(jsonString, DSQuery.class);
      if (!dsQuery.equals(deserializedDSQuery)) {
        log.error("Serialized and Deserialized DSQuery are not same for requestID: {}.",
            queryPayload.getRequestId());
        return queryPayload;
      }
      return newQueryPayload(queryPayload, deserializedDSQuery);
    } catch (Exception e) {
      log.error(MessageFormat.format("Deserialization failed for DS Query with requestId: {0}.",
          queryPayload.getRequestId()), e);
      return queryPayload;
    }
  }

  private static QueryPayload newQueryPayload(QueryPayload queryPayload,
      DSQuery dsQuery) {
    return QueryPayload.builder()
        .cacheKey(queryPayload.getCacheKey())
        .attemptKey(queryPayload.getAttemptKey())
        .clientId(queryPayload.getClientId())
        .deadLine(queryPayload.getDeadLine())
        .dsQuery(dsQuery) // Cosmos lib needs
        .params(queryPayload.getParams())  // Cosmos lib needs
        .nativeQuery(queryPayload.getNativeQuery())
        .priority(queryPayload.getPriority())
        .queryWeight(queryPayload.getQueryWeight())
        .storeIdentifier(queryPayload.getStoreIdentifier())
        .requestId(queryPayload.getRequestId())
        .dateRange(queryPayload.getDateRange())
        .metaDataPayload(queryPayload.getMetaDataPayload())
        .build();
  }
}
