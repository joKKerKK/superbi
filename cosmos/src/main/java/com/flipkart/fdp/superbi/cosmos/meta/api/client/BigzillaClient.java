package com.flipkart.fdp.superbi.cosmos.meta.api.client;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.fdp.superbi.cosmos.meta.BigzillaConf;
import java.io.IOException;
import java.util.List;
import javax.validation.constraints.Min;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;

/**
 * User: ayush.vora
 * Date: 5/30/14
 */
public class BigzillaClient {
    private static BigzillaConf config = new BigzillaConf();
    private final HttpClient httpClient;
    private final HttpHost targetHost;
    private final ObjectMapper objectMapper;
    private static final Logger LOGGER = getLogger(BigzillaClient.class);
    private final String selfHost;
    private final int selfPort;

    public BigzillaClient(HttpClient httpClient, BigzillaConf bigzillaConf) {
        config = bigzillaConf;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
        this.targetHost = new HttpHost(config.bigzillaHost.getHostName(),
                config.bigzillaHost.getPort(), "http");
        selfHost = config.selfHost;
        selfPort = config.selfPort;
    }

    //bad one, should get library or something
    public static class CreateRule {
        @NotEmpty
        @JsonProperty
        private final String uri;

        //NOTE can use dropwizard Duration
        @Min(1)
        @JsonProperty
        private final int rollingWindowMins;

        @Min(1)
        @JsonProperty
        private final int threshold;

        @NotEmpty
        @JsonProperty
        private final List<String> emailIds;

        @NotEmpty
        @JsonProperty
        private final List<String> companies;

        public CreateRule(String uri, List<String> companies) {
            this.uri = uri;
            this.companies = companies;

            this.rollingWindowMins = config.defaultRuleRollingWindowMins;
            this.threshold = config.defaultRuleThreshold;
            this.emailIds = newArrayList("bigfoot-oncall@flipkart.com");
        }
    }

    public JsonNode getErrorCountForDashboard() {
        try {
            HttpGet summaryGetRequest = new HttpGet("/v1/erroredIngestions");
            HttpResponse response = httpClient.execute(targetHost, summaryGetRequest);
            String responseBody = EntityUtils.toString(response.getEntity());
            return objectMapper.readTree(responseBody);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode getErroredIngestion(String fqnSubstring, int offset) {
        try {
            HttpGet erroredIngestionRequest =
                    new HttpGet("/v1/erroredIngestions/" + fqnSubstring + "?offset=" + offset);
            HttpResponse httpResponse = httpClient.execute(targetHost, erroredIngestionRequest);
            String responseBody = EntityUtils.toString(httpResponse.getEntity());
            JsonNode jsonNodeResponse = objectMapper.readTree(responseBody);
            String previous = jsonNodeResponse.path("paging").path("previous").textValue();
            String next = jsonNodeResponse.path("paging").path("next").textValue();

            ObjectNode newPaging = objectMapper.createObjectNode();

            //NOTE: hack to give iterator URLs
            if (previous != null) {
                newPaging.put("previous",
                        format("http://%s:%d/dart/failures/%s?offset=%s", selfHost, selfPort, fqnSubstring,
                                previous.split("offset=")[1]));
            }
            if (next != null) {
                newPaging.put("next",
                        format("http://%s:%d/dart/failures/%s?offset=%s", selfHost, selfPort, fqnSubstring,
                                next.split("offset=")[1]));
            }
            ObjectNode newResponse = objectMapper.createObjectNode();
            newResponse.set("erroredIngestions", jsonNodeResponse.path("erroredBEs"));
            newResponse.set("paging", newPaging);
            return newResponse;
        } catch (Exception e) {
            LOGGER.error("Error", e);
            throw new RuntimeException(e);
        }
    }

    /*public static void main(String[] args) {
        BigzillaConf bigzilla_conf = new BigzillaConf();
        bigzilla_conf.setBigzillaHost(new BigzillaConf.BigzillaHost("bigzilla.ch.flipkart.com", 28231));
        bigzilla_conf.setDefaultRuleRollingWindowMins(5);
        bigzilla_conf.setDefaultRuleThreshold(2);
        bigzilla_conf.selfHost = "bigfoot.ch.flipkart.com";
        bigzilla_conf.selfPort = 28221;
        BigzillaClient client = new BigzillaClient(HttpClientInitializer.getInstance(), bigzilla_conf);
        System.out.println(client.getErroredIngestion("Retail/Accounting/Material", 45));
    }*/

    /**
     * @deprecated
     */
    public JsonNode getAllErrorSummary() {
        try {
            HttpGet bigzillaSummaryRequest = new HttpGet("/erroredBE");
            HttpResponse httpResponse = httpClient.execute(targetHost, bigzillaSummaryRequest);
            return objectMapper.readTree(EntityUtils.toString(httpResponse.getEntity()));
        } catch (Exception e) {
            LOGGER.error("Error", e);
            throw new RuntimeException(e);
        }
    }

    public JsonNode getErrorSummary(String company, String org, String namespace, String name) {
        try {
            HttpGet bigzillaSummaryRequest =
                    new HttpGet(format("/v1/erroredIngestions/summary/%s/%s/%s/%s", company, org, namespace, name));
            HttpResponse httpResponse = httpClient.execute(targetHost, bigzillaSummaryRequest);
            return objectMapper.readTree(EntityUtils.toString(httpResponse.getEntity()));
        } catch (Exception e) {
            LOGGER.error("Error", e);
            throw new RuntimeException(e);
        }
    }

    private void setEntity(HttpEntityEnclosingRequestBase request, CreateRule obj) {
        try {
            String entity = this.objectMapper.writer().writeValueAsString(obj);
            StringEntity se = new StringEntity(entity, ContentType.APPLICATION_JSON);
            se.setContentType("application/json; charset=UTF-8");
            request.setEntity(se);
        } catch (IOException e) {
            throw new RuntimeException("Error while serializing: " + obj);
        }
    }
}
