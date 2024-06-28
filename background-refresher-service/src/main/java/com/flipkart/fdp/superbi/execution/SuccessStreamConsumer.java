package com.flipkart.fdp.superbi.execution;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.cosmos.transformer.CosmosServerTransformer;
import com.flipkart.fdp.superbi.d42.ChunkedInputStreamIterator;
import com.flipkart.fdp.superbi.d42.D42Uploader;
import com.flipkart.fdp.superbi.d42.IteratorToInputStreamAdapter;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.dsl.query.Schema;
import com.flipkart.fdp.superbi.execution.exception.DataSizeLimitExceedException;
import com.flipkart.fdp.superbi.gcs.GcsUploader;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.config.D42MetaConfig;
import com.flipkart.fdp.superbi.refresher.api.result.cache.QueryResultCachedValue;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import com.flipkart.fdp.superbi.refresher.api.result.query.RawQueryResult;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class SuccessStreamConsumer implements BiConsumer<BackgroundRefreshTask, QueryResult> {

    private static final String STREAM_SUCCESS_TIME_KEY = "success.stream.time";
    private static final String STREAM_SUCCESS_COUNTER_KEY = "success.stream.counter";
    private static final String RESULT_TOTAL_ROWS_ITERATION_TIME_KEY = "result.iterationTime.rows_total";
    private static final String RESULT_TOTAL_ROWS_HISTOGRAM_KEY = "result.rows_total";
    private static final String RESULT_TRUNCATED_ROWS_HISTOGRAM_KEY = "result.rows_truncated";
    private static final String RESULT_TRUNCATED_ROWS_TIMER_KEY = "result.time.rows_truncated";
    private static final String RESULT_D42_UPLOAD_TIME_KEY = "result.d42_upload.time";
    private static final String RESULT_TRUNCATED_METER_KEY = "result.truncate.meter";
    private static final String RESULT_ROW_TRANSFORMATION_TIME_KEY = "result.row.transformation.time";
    private static final String D42_SIZE_KEY = "result.d42.upload.size";
    public static final String FILE_FORMAT = ".csv";
    private static final String ROW_LIMIT_EXCEED_METER = "result.rowLimit.count";
    private final CacheDao cacheDao;
    private final D42Uploader d42Uploader;
    private final GcsUploader gcsUploader;
    private final MetricRegistry metricRegistry;
    private final D42MetaConfig d42MetaConfig;
    private final Map<String, Timer> truncatedRowsTimerForStore = new HashMap<>();
    private final StreamTransformer streamTransformer;
    private static final StreamTransformer DEFAULT_STREAM_TRANSFORMER = (originalResult, dsQuery, params, timer) ->
        new CosmosServerTransformer(originalResult, dsQuery, params, timer).postProcess();

    public SuccessStreamConsumer(CacheDao cacheDao, D42Uploader d42Uploader, GcsUploader gcsUploader,
                                 MetricRegistry metricRegistry, D42MetaConfig d42MetaConfig){
        this(cacheDao, d42Uploader, gcsUploader, metricRegistry, d42MetaConfig, DEFAULT_STREAM_TRANSFORMER);
    }

    @Override
    @SneakyThrows
    public void accept(BackgroundRefreshTask task, QueryResult queryResult) {
        log.info("accept called");
        final String storeIdentifier = task.getQueryPayload().getStoreIdentifier();
        Counter transformationCounter = getSuccessStreamCounter(storeIdentifier);
        Meter truncatedResultMeter = getResultTruncatedMeter(storeIdentifier);
        transformationCounter.inc();

        log.info("transforming result for cacheKey - <{}> and requestKey -<{}>", task.getCacheKey(), task.getQueryPayload().getRequestId());

        final StreamTransformer BIFURCATING_STREAM_TRANSFORMER = (originalResult, dsQuery, params, timer) -> {
            log.info("dsQuery : " + dsQuery);
            if(dsQuery != null) {
                log.info("dsQuery != null");
                return streamTransformer.apply(originalResult, dsQuery, params,timer);
            }
            log.info("originalResult : " + originalResult);
            return originalResult;
        };

        try(Timer.Context successStreamTimerContext = getSuccessStreamTimer(storeIdentifier);
            QueryResult transformedQueryResult = BIFURCATING_STREAM_TRANSFORMER.apply(queryResult,
                task.getQueryPayload().getDsQuery(), task.getQueryPayload().getParams(),
                getRowTransformationTimer(storeIdentifier))){
            List<List<Object>> truncatedRows;
            try (Timer.Context truncatedRowsTimer = getTruncatedRowsTimerForStore(storeIdentifier)) {
                truncatedRows = getTruncatedRows(transformedQueryResult, (int) task.getBackgroundRefresherConfig().getResponseTruncationSizeInBytes());
            }

            Integer totalRowCount = truncatedRows.size();
            log.info("totalRowCount : " + totalRowCount.toString());
            Histogram truncatedRowsHistogram = getTruncatedRowsHistogram(storeIdentifier);
            truncatedRowsHistogram.update(totalRowCount);
            String d42Link = StringUtils.EMPTY;
            log.info("transformedQueryResult.iterator().hasNext() : " + transformedQueryResult.iterator().hasNext());
            if (transformedQueryResult.iterator().hasNext()) {
                log.info("transformedQueryResult.iterator().hasNext()");
                truncatedResultMeter.mark();
                for (String client : d42MetaConfig.getD42UploadClients()) {
                    log.info("D42 Upload Client: {}", client);
                }
                log.info("task.getQueryPayload().getMetaDataPayload().getClient() : " + task.getQueryPayload().getMetaDataPayload().getClient());
                if (d42MetaConfig.getD42UploadClients().contains(task.getQueryPayload().getMetaDataPayload().getClient())) {
                    try (Timer.Context timer = getResultD42UploadTimer(storeIdentifier);) {
                        QueryResult intermediateQueryResult = concatQueryResult(truncatedRows,
                            transformedQueryResult);
                        D42Result d42Result = uploadCsvToD42(task, intermediateQueryResult,
                            d42Link);
                        totalRowCount = d42Result.getRowCount();
                        log.info("totalRowCount : " + totalRowCount);
                        d42Link = d42Result.getLink();
                        log.info("d42Link : " + d42Link);
                    }
                } else {
                    log.info("elseee");
                    try (Timer.Context timer = getTotalRowsIterationTimer(storeIdentifier);) {
                        Histogram totalRowsHistogram = getTotalRowsHistogram(storeIdentifier);
                        // we just need to count the items in iterator, no need to use
                        // transformedQueryResultIterator for this
                        int leftOutRowCount = Iterators.size(queryResult.iterator());
                        totalRowCount = totalRowCount + leftOutRowCount;
                        totalRowsHistogram.update(totalRowCount);
                    }
                }
            }
            handleTruncatedData(task, totalRowCount, truncatedRows,transformedQueryResult.getColumns(), d42Link);

            log.info(
                "transforming complete for cacheKey - <{}> and requestKey -<{}> for couchbase",
                task.getCacheKey(), task.getQueryPayload().getRequestId());
            task.unLockTask();
        }
    }

    private D42Result uploadCsvToD42(BackgroundRefreshTask task, QueryResult intermediateQueryResult,
        String d42Link) throws Exception {
        log.info("uploadCsvToD42 called");
        try (CSVStream csvStream = new CSVStream(intermediateQueryResult,task.getBackgroundRefresherConfig().getD42MaxSizeInMB()*1024*1024)) {
            String fileName = new StringBuilder().append(task.getCacheKey()).append(new Date().getTime()).toString();
            D42Result d42Result = uploadToD42(csvStream,fileName,d42MetaConfig.getD42ExpiryInSeconds(),task.getQueryPayload().getCacheKey());
            log.info("d42 Link -> <{}> generated for requestId- <{}>", d42Link,
                task.getQueryPayload().getRequestId());
            metricRegistry.gauge(MetricRegistry.name(D42_SIZE_KEY, task.getQueryPayload().getStoreIdentifier()),
                () -> () -> (int)(d42Result.getDataSize()/1024*1024));
            return d42Result;
        } catch (Exception e) {
            if(e instanceof DataSizeLimitExceedException){
                getDataLimitExceedMeter(task.getQueryPayload().getStoreIdentifier()).mark();
            }
            log.error("D42 upload failed due to ", e);
            throw e;
        }
    }

    private Meter getDataLimitExceedMeter(String storeIdentifier){
        return metricRegistry.meter(StringUtils.join(ROW_LIMIT_EXCEED_METER,storeIdentifier));
    }

    private Timer.Context getResultD42UploadTimer(String storeIdentifier) {
        return metricRegistry.timer(getMetricsKey(RESULT_D42_UPLOAD_TIME_KEY,storeIdentifier)).time();
    }

    private Timer.Context getTotalRowsIterationTimer(String storeIdentifier) {
        return metricRegistry.timer(getMetricsKey(RESULT_TOTAL_ROWS_ITERATION_TIME_KEY,storeIdentifier)).time();
    }

    private Histogram getTotalRowsHistogram(String storeIdentifier) {
        return metricRegistry.histogram(getMetricsKey(RESULT_TOTAL_ROWS_HISTOGRAM_KEY,storeIdentifier));
    }

    private Histogram getTruncatedRowsHistogram(String storeIdentifier) {
        return metricRegistry.histogram(getMetricsKey(RESULT_TRUNCATED_ROWS_HISTOGRAM_KEY,storeIdentifier));
    }

    private Timer getRowTransformationTimer(String storeIdentifier) {
        return metricRegistry.timer(getMetricsKey(RESULT_ROW_TRANSFORMATION_TIME_KEY,storeIdentifier));
    }

    private Timer.Context getSuccessStreamTimer(String storeIdentifier) {
        return metricRegistry.timer(getMetricsKey(STREAM_SUCCESS_TIME_KEY,storeIdentifier)).time();
    }

    private Meter getResultTruncatedMeter(String storeIdentifier) {
        return metricRegistry.meter(getMetricsKey(RESULT_TRUNCATED_METER_KEY,storeIdentifier));
    }

    private Counter getSuccessStreamCounter(String storeIdentifier) {
        return metricRegistry.counter(getMetricsKey(STREAM_SUCCESS_COUNTER_KEY,storeIdentifier));
    }

    private Timer.Context getTruncatedRowsTimerForStore(String storeIdentifier) {
        if (!truncatedRowsTimerForStore.containsKey(storeIdentifier)) {
            Timer timerForStore = metricRegistry.timer(getMetricsKey(RESULT_TRUNCATED_ROWS_TIMER_KEY, storeIdentifier));
            truncatedRowsTimerForStore.put(storeIdentifier, timerForStore);
        }
        return truncatedRowsTimerForStore.get(storeIdentifier).time();
    }

    private static QueryResult concatQueryResult(final List<List<Object>> truncatedRows, final QueryResult transformedQueryResult){
        return new QueryResult() {
            @Override
            public Iterator<List<Object>> iterator() {
                return Iterators.concat(truncatedRows.iterator(), transformedQueryResult.iterator());
            }

            @Override
            public List<String> getColumns() {
                return transformedQueryResult.getColumns();
            }

            @Override
            public void close() {
                transformedQueryResult.close();
            }
        };
    }

    private static List<List<Object>> getTruncatedRows(QueryResult queryResult, final int sizeLimitInBytes) {
        List<List<Object>> dataList = Lists.newArrayList();
        while (queryResult.iterator().hasNext() && dataList.size() < 1000) {
            dataList.add(queryResult.iterator().next());
        }
        Integer recordLength = getApproximateRowLimitForSize(dataList, sizeLimitInBytes);
        for (Integer counter = dataList.size(); counter < recordLength && queryResult.iterator().hasNext(); counter++) {
            dataList.add(queryResult.iterator().next());
        }
        return dataList;
    }


    private static List<String> getColumnHeaders(Schema schema) {
        return schema.columns.stream().map(selectCol -> selectCol.getAlias()).collect(Collectors.toList());
    }

    private D42Result uploadToD42(final CSVStream csvStream, final String fileName, final long expireAfterSeconds, String cacheKey) throws Exception {
        log.info("uploadToD42 called");
        final String fileNameWithExtension = fileName + FILE_FORMAT;

        Iterator<String> csvIterator = csvStream.iterator();
        Iterator<byte[]> csvByteIterator = Iterators.transform(csvIterator, s -> s.getBytes());

        IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(csvByteIterator,
            d42MetaConfig.getMaxRowSize(), csvStream.getRowSeperator());

        ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream,
            d42MetaConfig.getMaxSizePerChunk());
        String d42Link = StringUtils.EMPTY;
        if(gcsUploader.allowedDatastores.stream().anyMatch(cacheKey::contains)) {
            log.info("Uploading to GCS " + fileNameWithExtension);
            d42Link = gcsUploader.uploadInputStreamsToGcs(expireAfterSeconds,fileNameWithExtension,inputStreams);
            log.info("Uploading to GCS " + fileNameWithExtension + " complete");
        } else {
            d42Link = d42Uploader.uploadInputStreamsToD42(expireAfterSeconds,fileNameWithExtension,inputStreams);
        }
        log.info("d42Link : " + d42Link);
        log.info("csvStream.getRowCount() : " + csvStream.getRowCount());
        log.info("csvStream.getDataSize() : " + csvStream.getDataSize());
        return new D42Result(d42Link,csvStream.getRowCount(),csvStream.getDataSize());
    }

    private void handleTruncatedData(BackgroundRefreshTask task, Integer totalRowCount, List<List<Object>> truncatedResultBySize,List<String> columns,String d42Link) {
        log.info("handleTruncatedData called");
        RawQueryResult queryResult = RawQueryResult.builder()
            .dateHistogramMeta(task.getQueryPayload().getDateRange())
            .data(truncatedResultBySize)
            .columns(columns)
            .build();
        long completedAt = new Date().getTime();

        AttemptInfo attemptInfo = AttemptInfo.builder()
            .startedAtTime(completedAt)
            .finishedAtTime(completedAt)
            .cachedAtTime(completedAt)
            .errorMessage(null)
            .status(AttemptInfo.Status.SUCCESS)
            .attemptKey(task.getQueryPayload().getAttemptKey())
            .cacheKey(task.getQueryPayload().getCacheKey())
            .requestId(task.getQueryPayload().getRequestId())
            .refresherNodeAddress(getHostName())
            .serverError(false)
            ._debugInfo(Maps.newHashMap())
            .build();

        /*
            If DSQuery is present and contains limit, we use this limitValue
            If nativeQuery is there, we are not going to extract limit out of native query
            and can fallback on -1 or totalRows(Assuming that was the limit). Hence we choose to fallback on -1
         */
        int queryLimit = -1;
        if(task.getQueryPayload().getDsQuery() != null && task.getQueryPayload().getDsQuery().getLimit().isPresent()) {
            queryLimit =  task.getQueryPayload().getDsQuery().getLimit().get();
        }


        QueryResultCachedValue queryCachedResult = wrapQueryResultFromSinkForCaching(queryResult,
            task.getCacheKey(),
            completedAt, totalRowCount, truncatedResultBySize.size(),d42Link,queryLimit
        );

        String cacheKey = task.getCacheKey();
        cacheDao.set(task.getQueryPayload().getAttemptKey(),
            task.getBackgroundRefresherConfig().getResultCacheTtlInSec(), attemptInfo);

        cacheDao.set(cacheKey, task.getBackgroundRefresherConfig().getResultCacheTtlInSec(), queryCachedResult);
    }

    private static int getApproximateRowLimitForSize(List<List<Object>> list, long dataSizeLimit) {
        if (list.isEmpty()) {
            return 0;
        }
        final int modelRecordsLimitForSizeCalc = Math.min(list.size(), 1000);

        List modelList = list.subList(0, modelRecordsLimitForSizeCalc);
        long modelListSize = JsonUtil.getJsonSize(modelList);
        int modelListLen = modelList.size();

        return (int) (((modelListLen + 0.0) / modelListSize) * dataSizeLimit);
    }

    private static String getMetricsKey(String prefix, String storeIdentifier){
        return StringUtils.join(Arrays.asList(prefix,storeIdentifier),'.');
    }

    @SneakyThrows
    private QueryResultCachedValue wrapQueryResultFromSinkForCaching(
        RawQueryResult queryResultFromSink,
        String cacheKey, long cachedAtTime, int totalRows, int truncatedRows, String d42Link
        ,int queryLimit){
        boolean isTruncated = totalRows > truncatedRows;
        return QueryResultCachedValue.builder()
                .cacheKey(cacheKey)
                .cachedAtTime(cachedAtTime)
                .executingRefresherThreadId(Thread.currentThread().getName())
                .queryResult(queryResultFromSink)
                .truncatedRows(truncatedRows)
                .totalNumberOfRows(totalRows)
                .truncated(isTruncated)
                .refresherNodeAddress(getHostName())
                .d42Link(d42Link)
                .queryLimit(queryLimit)
                .build();
    }

    private static String getHostName() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "Unknown";
        }
    }

    @FunctionalInterface
    public interface StreamTransformer {
        QueryResult apply(QueryResult queryResult, DSQuery dsQuery, Map<String, String[]> params,Timer timer);

        default StreamTransformer andThen(StreamTransformer streamTransformer) {
            Objects.requireNonNull(streamTransformer);
            return (queryResult, dsQuery, params, timer) -> {
                return streamTransformer.apply(this.apply(queryResult, dsQuery, params,timer), dsQuery, params,timer);
            };
        }
    }


    @Getter
    @AllArgsConstructor
    class D42Result {
        private String link;
        private Integer rowCount;
        private long dataSize;

    }

}
