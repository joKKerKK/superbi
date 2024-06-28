package com.flipkart.fdp.superbi.d42;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import java.io.ByteArrayInputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@AllArgsConstructor
@Slf4j
public class D42Uploader {

  private final D42Client d42Client;
  private final CircuitBreaker circuitBreaker;
  private final Retry retry;
  private final MetricRegistry metricRegistry;

  private static final String D42_COMPLETE_UPLOAD_TIME_KEY = "d42.upload.complete.time";
  private static final String D42_PART_UPLOAD_TIME_KEY = "d42.upload.part.time";

  public String uploadInputStreamsToD42(long expireAfterSeconds, String fileNameWithExtension,ChunkedInputStreamIterator inputStreams){
    log.info("uploadInputStreamsToD42 called");
    log.info("fileNameWithExtension : " + fileNameWithExtension);
    String d42Link = StringUtils.EMPTY;
    try {
      AmazonS3 s3Client = d42Client.getD42Connection();
      uploadToD42(fileNameWithExtension, inputStreams,s3Client);
      Long expiryInMillies = new Date().getTime() + expireAfterSeconds * 1000;
      d42Link = d42Client.createD42Link(s3Client,fileNameWithExtension, expiryInMillies);
      log.info("d42Link : " + d42Link);
      return d42Link;
    }catch (Exception e){
      log.error(MessageFormat.format("D42 upload/link generation failed for file {0} due to {1}",fileNameWithExtension,e.getMessage()));
      throw e;
    }
  }

  private void uploadToD42(String fileNameWithExtension,
      ChunkedInputStreamIterator inputStreams,AmazonS3 s3Client) {
    log.info("uploadToD42 called");
    String uploadId = initiateUploadRequest(fileNameWithExtension, s3Client);
    log.info("uploadId : " + uploadId);

    try(Timer.Context uploadTime = metricRegistry.timer(D42_COMPLETE_UPLOAD_TIME_KEY).time()){
      log.info("uploadTime : " + uploadTime);
      List<PartETag> partETags = uploadInputStreams(fileNameWithExtension, s3Client, uploadId,
          inputStreams);

      CompleteMultipartUploadRequest completeRequest = new
          CompleteMultipartUploadRequest(
          d42Client.getBucket(),
          fileNameWithExtension,
          uploadId,
          partETags);
      s3Client.completeMultipartUpload(completeRequest);
      log.info(MessageFormat.format("D42 upload successful for file {0}",fileNameWithExtension));
    }catch (Exception e){
      AbortMultipartUploadRequest abortMultipartUploadRequest = new
          AbortMultipartUploadRequest(d42Client.getBucket(),fileNameWithExtension,uploadId);
      s3Client.abortMultipartUpload(abortMultipartUploadRequest);
      log.error(MessageFormat.format("D42 upload failed for file {0} due to {1}",fileNameWithExtension,e.getMessage()));
      throw e;
    }
  }

  private List<PartETag> uploadInputStreams(String fileNameWithExtension, AmazonS3 s3Client,
      String uploadId, ChunkedInputStreamIterator inputStreams) {
    log.info("uploadInputStreams called");
    List<PartETag> partETags = Collections.synchronizedList(new ArrayList<PartETag>());
    int partNumber = 1;
    for(ByteArrayInputStream inputStream: inputStreams) {
      PartETag partETag = uploadPartRequest(fileNameWithExtension, s3Client, uploadId,
          partNumber, inputStream);
      partNumber++;
      partETags.add(partETag);
    }
    return partETags;
  }

  private PartETag uploadPartRequest(String fileNameWithExtension, AmazonS3 s3Client,
      String uploadId, int partNumber, ByteArrayInputStream inputStream) {
    try (Timer.Context uploadTime = metricRegistry.timer(D42_PART_UPLOAD_TIME_KEY).time()) {
      UploadPartRequest uploadRequest = new UploadPartRequest()
          .withBucketName(d42Client.getBucket()).withKey(fileNameWithExtension)
          .withUploadId(uploadId).withPartNumber(partNumber)
          .withInputStream(inputStream)
          .withPartSize(inputStream.available()); //Because it is ByteArrayInputStream, available is total buffer
      UploadPartResult uploadPartResult = CircuitBreaker.decorateFunction(circuitBreaker,
          Retry.decorateFunction(retry,s3Client::uploadPart)).apply(uploadRequest);
      return uploadPartResult.getPartETag();
    }
  }

  private String initiateUploadRequest(String fileNameWithExtension, AmazonS3 s3Client) {
    log.info("initiateUploadRequest called");
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(d42Client.getBucket(),
        fileNameWithExtension);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    return initResponse.getUploadId();
  }
}
