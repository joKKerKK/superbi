package com.flipkart.fdp.superbi.cosmos.dg.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.fdp.superbi.cosmos.dg.exception.BadHttpRequest;
import com.flipkart.fdp.superbi.cosmos.dg.exception.InternalErrorException;
import com.flipkart.fdp.superbi.cosmos.dg.exception.ServiceUnavailableException;
import com.flipkart.fdp.superbi.cosmos.dg.models.IngestibleEntity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * Created by arun.khetarpal on 08/08/15.
 */
public class BatchHttpIngestor implements BatchIngestionStrategy {

    private HttpClient httpClient;
    private HttpPost httpPost;

    private String uri;
    private String entity;
    private List<IngestibleEntity> entityList;

    public BatchHttpIngestor(String uri, String entity) {
        this.uri = uri;
        this.entity = entity;
    }

    @Override
    public void init() {
        httpPost = new HttpPost(uri);
        httpClient = new DefaultHttpClient();
        entityList = new ArrayList<IngestibleEntity>();
        httpPost.addHeader("Content-Type", "application/json");
    }

    @Override
    public void ingest(IngestibleEntity entity) {
        entityList.add(entity);
    }

    @Override
    public void ingest(List<IngestibleEntity> entities) {
        entityList.addAll(entities);
    }

    private String constructEntity() throws JsonProcessingException {
        return String.format("{\"%s\": %s}", entity, new ObjectMapper().writeValueAsString(entityList));
    }

    private void postProcess(HttpResponse response) throws BadHttpRequest, ServiceUnavailableException,
                                                    InternalErrorException {
        switch (response.getStatusLine().getStatusCode()) {
            case 200: return;
            case 400: throw new BadHttpRequest(response.getStatusLine().getReasonPhrase());
            case 503: throw new ServiceUnavailableException(response.getStatusLine().getReasonPhrase());
            default:  throw new InternalErrorException(response.getStatusLine().getReasonPhrase());
        }
    }

    @Override
    public String commit() throws InternalErrorException {
        try {
            httpPost.setEntity(new StringEntity(constructEntity()));
            HttpResponse response = httpClient.execute(httpPost);
            postProcess(response);
            return EntityUtils.toString(response.getEntity());
        } catch(IOException | RuntimeException ex) {
            throw new InternalErrorException(ex);
        } finally {
            init(); // reinit
        }
    }
}
