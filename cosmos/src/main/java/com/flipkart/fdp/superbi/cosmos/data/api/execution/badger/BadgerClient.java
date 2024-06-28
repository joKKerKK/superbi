package com.flipkart.fdp.superbi.cosmos.data.api.execution.badger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.BadgerProcessData;
import com.flipkart.fdp.superbi.cosmos.data.api.execution.badger.responsepojos.TableCatalogInfo;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.google.inject.Singleton;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.json.JSONException;

import javax.ws.rs.HttpMethod;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

@Singleton
public class BadgerClient {
    public static final String SERVICE_NAME = "BADGER_SERVICE";
    private static final String GET_TABLE_CATALOG_INFO = "/v1/catalog/tables/";
    private static final String GET_ALL_ACTIVE_PROCESS_DATA = "/view/data/";
    private String host;
    private final Integer port;
    private final AsyncHttpClient client;

    private static BadgerClient instance;

    public static BadgerClient getInstance() {
        return instance;
    }

    // Fix - Should be initialized only once
    public static void setInstance(BadgerClient instance) {
        BadgerClient.instance = instance;
    }

    public BadgerClient(AsyncHttpClient asyncHttpClient, String host, Integer port) {
        this.client = asyncHttpClient;
        this.host = host;
        this.port = port;
    }

    @SneakyThrows
    public List<TableCatalogInfo> getTableCatalogInfo(String factName, String metaStoreType, String tableType){
        ObjectMapper objectMapper = new ObjectMapper();
        Request request = new RequestBuilder(HttpMethod.GET).setUrl(host + ":" + port  + GET_TABLE_CATALOG_INFO + factName + "?metastoreType=" + metaStoreType +"&tableType=" + tableType)
                .build();
        Response response = client.executeRequest(request).toCompletableFuture().get();
        String result = String.valueOf(response.getResponseBody());
        try {
            return StringUtils.isBlank(result)? Collections.emptyList() : objectMapper.readValue(result, objectMapper.getTypeFactory().constructCollectionType(List.class, TableCatalogInfo.class));
        } catch (JSONException e) {
            throw new ServerSideException(
                    MessageFormat.format("Badger response object is not proper for {0} due to {1} ", factName,
                            e.getMessage()));
        }
    }

    @SneakyThrows
    public List<BadgerProcessData> getAllActiveProcessData(String org, String namespace, String name){
        ObjectMapper objectMapper = new ObjectMapper();
        Request request = new RequestBuilder(HttpMethod.GET).setUrl(host + ":" + port  + GET_ALL_ACTIVE_PROCESS_DATA + org + "_" +  namespace + "/" + name)
            .build();
        Response response = client.executeRequest(request).toCompletableFuture().get();
        String result = String.valueOf(response.getResponseBody());
        try {
            return StringUtils.isBlank(result)? Collections.emptyList() : objectMapper.readValue(result, objectMapper.getTypeFactory().constructCollectionType(List.class, BadgerProcessData.class));
        } catch (JSONException e) {
            throw new ServerSideException(
                MessageFormat.format("Badger response object is not proper for {0} due to {1} ", name,
                    e.getMessage()));
        }
    }
}
