package com.flipkart.fdp.superbi.gcs;

import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.fdp.superbi.d42.ChunkedInputStreamIterator;
import com.flipkart.fdp.utils.cfg.ConfigBucketKey;
import com.google.auth.oauth2.IdentityPoolCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import io.grpc.Context;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GcsUploader {
    private final GcsConfig gcsConfig;
    private final Storage storage;
    private final MetricRegistry metricRegistry;
    private static final String D42_COMPLETE_UPLOAD_TIME_KEY = "d42.upload.complete.time";
    private static final String SCOPES = "https://www.googleapis.com/auth/cloud-platform";
    public final ArrayList<String> allowedDatastores;

    public GcsUploader(GcsConfig gcsConfig, MetricRegistry metricRegistry) {
        this.gcsConfig = gcsConfig;
        this.allowedDatastores = gcsConfig.getAllowedDatastores();
        this.storage = getStorage(gcsConfig);
        this.metricRegistry = metricRegistry;
    }

    @SneakyThrows
    private Storage getStorage(GcsConfig gcsConfig) {

        GoogleCredentials credentials = ServiceAccountCredentials.getApplicationDefault();
        IdentityPoolCredentials identityPoolCredentials = (IdentityPoolCredentials) credentials;
        String serviceAccountEmail = identityPoolCredentials.getServiceAccountEmail();
        ImpersonatedCredentials impersonatedCredentials = ImpersonatedCredentials.create(credentials, serviceAccountEmail,
                new ArrayList<>(), Lists.newArrayList(SCOPES), 500);

        return StorageOptions.newBuilder()
                .setProjectId(gcsConfig.getProjectId())
                .setCredentials(impersonatedCredentials)
                .build().getService();
    }

    public String uploadInputStreamsToGcs(long expireAfterSeconds, String fileNameWithExtension, ChunkedInputStreamIterator inputStreams){
        log.info("uploadInputStreamsToGcs called");
        String gcsLink = StringUtils.EMPTY;
        try(Timer.Context uploadTime = metricRegistry.timer(D42_COMPLETE_UPLOAD_TIME_KEY).time()) {
            gcsLink = uploadFile(gcsConfig.getBucket(), fileNameWithExtension, concatenateInputStreams(inputStreams), expireAfterSeconds);
            log.info("gcsLink : " + gcsLink);
            return gcsLink;
        }catch (Exception e){
            log.error(MessageFormat.format("GCS upload/link generation failed for file {0} due to {1}",fileNameWithExtension,e.getMessage()));
            throw e;
        }
    }

    public String uploadFile(String gcsBucket, String objectName, InputStream inputStream, long expireAfterSeconds) {
        log.info("uploadFile called");
        log.info("gcsBucket : " + gcsBucket);
        log.info("objectName : " + objectName);
        log.info("expireAfterSeconds : " + expireAfterSeconds);

        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(gcsBucket, objectName)).build();
        try {
            Blob blob = storage.createFrom(blobInfo, inputStream);
            URL signedUrl = blob.signUrl(expireAfterSeconds, TimeUnit.SECONDS, Storage.SignUrlOption.withV4Signature());
            log.info("signedUrl : " + signedUrl.toString());
            return signedUrl.toString();
        } catch (IOException e) {
            log.error("Error during file upload " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static InputStream concatenateInputStreams(ChunkedInputStreamIterator inputStreams) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (ByteArrayInputStream inputStream : inputStreams) {
            try {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
    }
}
