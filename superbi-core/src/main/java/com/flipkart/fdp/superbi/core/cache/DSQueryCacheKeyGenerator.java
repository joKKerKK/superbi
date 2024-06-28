package com.flipkart.fdp.superbi.core.cache;

import com.flipkart.fdp.superbi.core.model.DSQueryRefreshRequest;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DSQueryCacheKeyGenerator extends CacheKeyGenerator<DSQueryRefreshRequest>{

  public String getCacheKey(DSQuery dsQuery, Map<String, String[]> params,
      String sourceIdentifier) {
    DSQueryRefreshRequest dsQueryRefreshRequest = DSQueryRefreshRequest.builder()
        .dsQuery(dsQuery)
        .params(params)
        .storeIdentifier(sourceIdentifier)
        .build();

    return getCacheKey(dsQueryRefreshRequest, sourceIdentifier);
  }

  @Override
  public String getCacheKey(DSQueryRefreshRequest queryRefreshRequest, String sourceIdentifier) {

    DSQuery dsQuery = queryRefreshRequest.getDsQuery();
    Map<String, String[]> params = queryRefreshRequest.getParams();

    String dsQueryString,paramsString,dateRangeString ;

    dsQueryString = JsonUtil.toJson(dsQuery);
    dateRangeString = JsonUtil.toJson(dsQuery.getDateRange(params));
    paramsString = JsonUtil.toJson(new TreeMap<>(params).entrySet().stream()
        .filter(map-> !(map.getKey().contains("DummyParam"))).collect(
            Collectors.toMap(map -> map.getKey(), map -> map.getValue())));
    final StringBuilder cacheKey = new StringBuilder();
    cacheKey.append(dsQueryString).append(dateRangeString).append(paramsString);

    return "superbi_" + sourceIdentifier + "_" + toMD5(cacheKey.toString());

   }
}
