package com.flipkart.fdp.superbi.cosmos.meta.api.client;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.String.format;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.cosmos.meta.api.DartEntityResource;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.DartIngestionDefRequest;
import com.flipkart.fdp.superbi.cosmos.meta.util.DartException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: aartika
 * Date: 4/22/14
 */
public class DartClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final HttpHost targetHost;
    private static final String DEF_PATH = "/ingestiondef/upload";

    //TODO will go away once STATUS_PATH_NEW is finalized
    private static final String STATUS_PATH = "/ingestionInfo/ingestion/status/all";
    private static final String STATUS_PATH_NEW = "/ingestionInfo/v2/ingestion/status/";
    private static final String METRIC_PATH = "/ingestionInfo/ingestion/metricUrl/all";
    private static final Joiner COMMA_JOINER = Joiner.on(',');
    private static final String BOOTSTRAP_PATH_FORMAT = "/ingestion/%s/%s/%s/%s/bootstrapComplete";
    private static final String ACTIVATE_INGESTION_PATH_FORMAT = "/ingestion/%s/%s/%s/%s/changeTopicStatus";
    private static final String DARTBOARD_PATH = "/ingestionInfo/metrics/counts/all";
    private static final String DARTBOARD_HIST_PATH = "/ingestionInfo/metrics/countsHistory/";
    private static final Joiner ENTITY_URI_JOINER = Joiner.on('/');
    private static final String DEFAULT_METRICS_HOST = "10.47.2.91";
    private static final String DEFAULT_DETAIL_METRICS_HOST = "10.47.2.71";


  private static final Logger LOGGER = LoggerFactory.getLogger(DartClient.class);

    public DartClient(HttpClient httpClient, String host, int port) {
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.targetHost = new HttpHost(host, port, "http");
    }

    public String updateDartEntity(DartIngestionDefRequest entity, DartEntityResource.UpdateType updateType) {
        LOGGER.info("Updating Dart Entity");
        return uploadDartEntity(entity, new HttpPost(format("%s/%s/%s", DEF_PATH,
                UploadType.update, updateType)));
    }

    public String createDartEntity(DartIngestionDefRequest entity) {
        LOGGER.info("Creating Dart Entity");
        return uploadDartEntity(entity, new HttpPost(format("%s/%s", DEF_PATH, UploadType.create)));
    }

    public String uploadDartEntity(DartIngestionDefRequest entity, HttpPost post) {
        setEntity(post, entity);
        try {
            HttpResponse response = this.httpClient.execute(this.targetHost, post);
            String responseStr = EntityUtils.toString(response.getEntity());
            LOGGER.trace("response for {}, {}", entity, responseStr);
            JsonNode retVal = objectMapper.readTree(responseStr);
            if (ServiceResponse.SUCCESS.equals(ServiceResponse.valueOf(retVal.get("status").asText())))
                return retVal.get("version").asText();
            else
                throw new DartException(responseStr);
        } catch (Exception e) {
            throw new RuntimeException("Error while registering with dart: " + e.toString(), e);
        }
    }

    private void setEntity(HttpEntityEnclosingRequestBase request, DartIngestionDefRequest obj) {
        try {
            String entity = this.objectMapper.writer().writeValueAsString(obj);
            StringEntity se = new StringEntity(entity, ContentType.APPLICATION_JSON);
            se.setContentType("application/json; charset=UTF-8");
            request.setEntity(se);
        } catch (IOException e) {
            throw new RuntimeException("Error while serializing: " + obj);
        }
    }

    public Map<String, Status> getAllDartStatus() {
        HttpGet get = new HttpGet(STATUS_PATH);
        try {
            HttpResponse response = this.httpClient.execute(this.targetHost, get);
            String responseStr = EntityUtils.toString(response.getEntity());
            TypeReference<HashMap<String, ResponseStatus>> typeRef
                    = new TypeReference<HashMap<String, ResponseStatus>>() {
            };
            Map<String, ResponseStatus> retVal = objectMapper.readValue(responseStr, typeRef);
            Map<String, Status> statusMap = new HashMap<String, Status>();
            for (Map.Entry<String, ResponseStatus> entry : retVal.entrySet()) {
//                String[] split = entry.getKey().split("/");
                String uri = entry.getKey().toLowerCase();
                if (statusMap.containsKey(uri)) {
                    statusMap.put(uri, statusMap.get(uri).and(entry.getValue().getStatus()));
                } else {
                    statusMap.put(uri, entry.getValue().getStatus());
                }
            }
            return statusMap;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode getAllDartStatusNew() {
        HttpGet get = new HttpGet(STATUS_PATH_NEW + "all");
        try {
            String responseStr = EntityUtils.toString(httpClient.execute(targetHost, get).getEntity());
            TypeReference<HashMap<String, JsonNode>> typeRef
                    = new TypeReference<HashMap<String, JsonNode>>() {
            };
            Map<String, JsonNode> fromDartWithCompanyMap = objectMapper.readValue(responseStr, typeRef);
            Map<String, JsonNode> toReturnValue = newHashMap();

            for (Map.Entry<String, JsonNode> stringJsonNodeEntry : fromDartWithCompanyMap.entrySet()) {
//                String[] split = stringJsonNodeEntry.getKey().split("/");
                String uri = stringJsonNodeEntry.getKey().toLowerCase();
                if (toReturnValue.containsKey(uri)) {
                    ObjectNode changedNode = objectMapper.createObjectNode();
                    changedNode.put("ingestionStatus",
                            Status.of(toReturnValue.get(uri)).and(Status.of(stringJsonNodeEntry.getValue())).name());
                    String existentBootstrapMode =
                            toReturnValue.get(uri).path("bootstrapStatus").path("bootstrapMode").asText();
                    if (existentBootstrapMode.equals("OFF")) {
                        changedNode.put("bootstrapStatus", toReturnValue.get(uri).path("bootstrapStatus"));
                    } else {
                        changedNode.put("bootstrapStatus", stringJsonNodeEntry.getValue().path("bootstrapStatus"));
                    }
                    toReturnValue.put(uri, changedNode);
                } else {
                    toReturnValue.put(uri, stringJsonNodeEntry.getValue());
                }
            }
            return objectMapper.createObjectNode().putAll(toReturnValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode getDartStatus(String company, String org, String namespace, String name) {
        try {
            HttpGet get = new HttpGet(STATUS_PATH_NEW + format("%s/%s/%s/%s", company, org, namespace, name));
            HttpResponse statusResponse = httpClient.execute(targetHost, get);
            final String statusResponseContent = EntityUtils.toString(statusResponse.getEntity());
            return objectMapper.readTree(statusResponseContent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, UrlsBean> getAllMetricsUrlsModified() {
        HttpGet get = new HttpGet(METRIC_PATH);
        try {
            HttpResponse response = this.httpClient.execute(this.targetHost, get);
            String responseStr = EntityUtils.toString(response.getEntity());
            TypeReference<HashMap<String, MetricInfo>> typeRef
                    = new TypeReference<HashMap<String, MetricInfo>>() {
            };
            Map<String, MetricInfo> retVal = objectMapper.readValue(responseStr, typeRef);
            Map<String, UrlsBean> metricUrls = new HashMap<String, UrlsBean>();
            for (Map.Entry<String, MetricInfo> metric : retVal.entrySet()) {

                UrlsBean urlsBean = new UrlsBean();
                String host = metric.getValue().getHost();
                String detailHost = metric.getValue().getDetailHost();
                String basicUrl = format("http://%s/q?m=sum:prod-fdpingestion.jmx.metrics."
                    + "%s.times.OneMinuteRate&nokey&smooth=csplines&png&wxh=1420x640",
                  (host != null && !host.isEmpty())?host:DEFAULT_METRICS_HOST,
                    metric.getValue().getMetricName());
                  urlsBean.setPreviewUrl(basicUrl);

                  String topicName = metric.getKey().replace("/",".");
                  urlsBean.setDetailedGraphUrl(format("http://%s/#/dashboard/script/fdp-ingestion-topic.js?topic=%s",
                    (detailHost != null && !detailHost.isEmpty())?host:DEFAULT_DETAIL_METRICS_HOST,topicName));
                  metricUrls.put(metric.getKey(), urlsBean);
            }
            return metricUrls;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode bootstrap(String company, String org, String namespace, String name) {
        try {
            HttpPut put = new HttpPut(format(BOOTSTRAP_PATH_FORMAT, company, org, namespace, name));
            HttpResponse dartBootstrapResponse = httpClient.execute(targetHost, put);
            final String dartBootstrapResponseContent = EntityUtils.toString(dartBootstrapResponse.getEntity());
            return objectMapper.readTree(dartBootstrapResponseContent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode changeSourceStatus(String company, String org, String namespace, String name, String source, String status) {
        try {

            Map<Source,Status> enableSourceStatus = Arrays.asList(source.split(","))
                    .stream()
                    .collect(Collectors.toMap(Source::valueOf, s -> Status.ACTIVE));

            for(Source src: Source.values()) {
                if(enableSourceStatus.containsKey(src) || src == Source.ALL) continue;
                enableSourceStatus.put(src, Status.SUSPENDED);
            }

            HttpPut put = new HttpPut(
                    format(ACTIVATE_INGESTION_PATH_FORMAT, company, org, namespace, name));

            String entity = this.objectMapper.writer().writeValueAsString(enableSourceStatus);
            StringEntity se = new StringEntity(entity, ContentType.APPLICATION_JSON);
            se.setContentType("application/json; charset=UTF-8");
            put.setEntity(se);

            HttpResponse dartBootstrapResponse = httpClient.execute(targetHost, put);
            final String dartBootstrapResponseContent = EntityUtils.toString(dartBootstrapResponse.getEntity());
            return objectMapper.readTree(dartBootstrapResponseContent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode getDartboardMetrics() {
        try {
            HttpGet get = new HttpGet(DARTBOARD_PATH);
            HttpResponse response = this.httpClient.execute(this.targetHost, get);
            String responseStr = EntityUtils.toString(response.getEntity());
            return objectMapper.readTree(responseStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode getDartboardMetricsHistory(final String company,
                                               final String org,
                                               final String namespace,
                                               final String name,
                                               final String start,
                                               final Optional<String> end) {
        try {
            HttpGet get = new HttpGet(
                    DARTBOARD_HIST_PATH + ENTITY_URI_JOINER.join(company, org, namespace, name) + "?start=" + start +
                            end.transform(new Function<String, String>() {
                                @Nullable
                                @Override
                                public String apply(String end) {
                                    return "&end=" + end;
                                }
                            }).or(""));
            HttpResponse response = this.httpClient.execute(this.targetHost, get);
            String responseStr = EntityUtils.toString(response.getEntity());
            return objectMapper.readTree(responseStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO horrible code :(
    public JsonNode getDartboardMetricsHistoryForNamespace(String company,
                                                           String org,
                                                           String namespace,
                                                           String start,
                                                           Optional<String> end) {
        try {
            HttpGet get = new HttpGet(
                    DARTBOARD_HIST_PATH + ENTITY_URI_JOINER.join(company, org, namespace) + "?start=" + start +
                            end.transform(new Function<String, String>() {
                                @Nullable
                                @Override
                                public String apply(String end) {
                                    return "&end=" + end;
                                }
                            }).or(""));
            HttpResponse response = this.httpClient.execute(this.targetHost, get);
            String responseStr = EntityUtils.toString(response.getEntity());
            return objectMapper.readTree(responseStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public enum UploadType {
        create,
        update
    }

    public enum ServiceResponse {
        SUCCESS,
        FAILURE
    }

    public enum Status {
        ACTIVE {
            @Override
            public Status and(Status status) {
                return ACTIVE;
            }
        },
        SUSPENDED {
            @Override
            public Status and(Status status) {
                if (status.equals(ACTIVE))
                    return ACTIVE;
                return SUSPENDED;
            }
        },
        KILLED {
            @Override
            public Status and(Status status) {
                if (status.equals(UNKNOWN))
                    return KILLED;
                return status;
            }
        },
        UNKNOWN {
            @Override
            public Status and(Status status) {
                return status;
            }
        };

        public abstract Status and(Status status);

        public static Status of(JsonNode jsonNode) {
            return valueOf(jsonNode.path("ingestionStatus").asText());
        }
    }

    public enum Source {
        DART,
        SPECTER,
        BATCH_INGESTION,
        ALL;
    }

    @Setter
    @Getter
    public static class ResponseStatus {

        @JsonProperty
        private Status status;

        public ResponseStatus() {
        }

    }

    @Getter
    @Setter
    public static class MetricInfo {

        @JsonProperty
        private String metricName;

        @JsonProperty
        private String host;

        @JsonProperty
        private String detailHost;

        @JsonProperty
        private int port;

        @JsonProperty
        private String metricStore;

        public MetricInfo() {
        }
    }

    @Getter
    @Setter
    public static class UrlsBean {

        @JsonProperty
        private String previewUrl;

        @JsonProperty
        private String detailedGraphUrl;

        public UrlsBean() {
        }
    }

}
