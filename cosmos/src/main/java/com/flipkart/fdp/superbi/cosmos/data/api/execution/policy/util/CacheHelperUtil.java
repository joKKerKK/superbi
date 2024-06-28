package com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.policy.ExecutionContext;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by arun.khetarpal on 25/11/15.
 */
public class CacheHelperUtil {
    private static final Logger logger = LoggerFactory.getLogger(CacheHelperUtil.class);

    public static long getLastFactCreatedTime(String tableName) {

        long factCreatedAtTime = 0;

        Date lastRefreshDate =  MetaAccessor.get().getTableByName(tableName.split("\\.").length == 1 ?
                MetaAccessor.get().getFactByName(tableName).getTableName() : tableName)
                .getLastRefresh();

        if (lastRefreshDate != null) {
            factCreatedAtTime = lastRefreshDate.getTime();
        }
        return factCreatedAtTime;
    }

    public static String createKey(Object nativeQuery, DSQuery query, ExecutionContext context) {
        Iterable<SelectColumn> derivedColumns = query.getDerivedColumns();
        final StringBuilder cacheKey = new StringBuilder();
        try {
            nativeQuery = new ObjectMapper().writeValueAsString(nativeQuery);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to generate key for query", e);
        }
        cacheKey.append(nativeQuery);
        for (SelectColumn derivedColumn : derivedColumns) {
            cacheKey.append("|||")
                    .append(((SelectColumn.Expression) derivedColumn).expressionString);
        }

        Map<String, String[]> param = context.getParams();
        if (query.getDateHistogramCol().isPresent() && !query.hasGroupBys()) {
            SelectColumn.DateHistogram dateHistogram = query.getDateHistogramCol().get();
            if (dateHistogram.getSeriesType(param).equals(SelectColumn.SeriesType.CUMULATIVE)) {
                cacheKey.append(String.valueOf(SelectColumn.SeriesType.CUMULATIVE));
            } else if (dateHistogram.getSeriesType(param).equals(SelectColumn.SeriesType.GROWTH)) {
                cacheKey.append(String.valueOf(SelectColumn.SeriesType.GROWTH));
            }
        }

        for (SelectColumn col : query.getSelectedColumns())
            cacheKey.append(col.getAlias() + ":" + col.isVisible());

        return toMD5(cacheKey.toString());
    }

    private static String toMD5(String key) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte byteData[] = messageDigest.digest(key.getBytes());

            StringBuffer hexString = new StringBuffer();
            for (int i = 0; i < byteData.length; i++) {
                String hex = Integer.toHexString(0xff & byteData[i]);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException nsae) {
            logger.error("Error while computing message digest for " +
                    "key: {} error: {}", key, nsae);
            throw new RuntimeException(nsae);
        }
    }
}
