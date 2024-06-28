package com.flipkart.fdp.superbi.d42;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class D42Client {

  private final String d42Endpoint;
  private final String accessKey;
  private final String secretKey;

  @Getter
  private final String bucket;

  private final String projectId;


  public AmazonS3 getD42Connection() {
    BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setSignerOverride("AWSS3V4SignerType");
    AmazonS3 interopClient =
        AmazonS3ClientBuilder.standard()
            .withClientConfiguration(clientConfig)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    d42Endpoint, "auto"))
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRequestHandlers(new RequestHandler2() {
              @Override
              public void beforeRequest(Request<?> request) {
                request.addHeader("x-amz-project-id", projectId);
              }
            })
            .build();
    return interopClient;
  }

  public String createD42Link(AmazonS3 conn, String key, Long expiryInMillies) {
    GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, key);
    request.setExpiration(new Date(expiryInMillies));
    return conn.generatePresignedUrl(request).toString();
  }
}
