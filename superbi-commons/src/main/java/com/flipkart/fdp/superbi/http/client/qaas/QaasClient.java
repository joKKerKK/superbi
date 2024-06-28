package com.flipkart.fdp.superbi.http.client.qaas;

import com.flipkart.fdp.qaas.client.QAASClient;
import com.flipkart.fdp.qaas.client.QAASClientConfig;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import io.swagger.client.model.QuerySubmitResult;
import io.swagger.client.model.ValidationResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class QaasClient {

  private final QaasConfiguration configuration;
  private final Long blockingTimeOut;
  private final String downloadEndPoint;
  private QAASClient qaasClient;
  private QAASClientConfig qaasClientConfig;

  public QaasClient(QaasConfiguration configuration) {
    this.configuration = configuration;
    this.blockingTimeOut = configuration.getBlockingTimeout();
    this.downloadEndPoint = configuration.getDownloadEndPoint();
  }

  @SneakyThrows
  public QaasDownloadResponse getDownloadURLForReport(String source, String query, String username, String sourceType) {
    this.qaasClientConfig = getQaasClientConfig(configuration, username);
    this.qaasClient = new QAASClient(qaasClientConfig);
    ValidationResponse validationResponse = qaasClient.api.validate(query, sourceType);
    if (Objects.isNull(validationResponse)) {
      throw new ServerSideException(String.format("Null Response received from Qaas query validation, query: %s", query));
    }
    if (!validationResponse.getSucceeded()) {
      throw new ClientSideException(String.format("Validation failed for query: %s. Error: %s", query, validationResponse.getMessage()));
    }
    QuerySubmitResult querySubmitResult = qaasClient.api.execute(query, blockingTimeOut, sourceType, "");
    if (Objects.isNull(querySubmitResult) || Objects.isNull(querySubmitResult.getQueryHandle())) {
      throw new ServerSideException(String.format("Unable to download report from Qaas for query '%s', source '%s'", query, source));
    }
    return new QaasDownloadResponse(String.format("%s/%s", this.downloadEndPoint, querySubmitResult.getQueryHandle().getHandle()));
  }

  private QAASClientConfig getQaasClientConfig(QaasConfiguration config, String username) {
    return QAASClientConfig.builder().host(config.getHost()).
                                      username(username).
                                      clientID(config.getClientId()).
                                      clientSecret(config.getClientSecret()).
                                      connectTimeOutMillis(config.getConnectTimeout()).
                                      readTimeOutMillis(config.getReadTimeout()).
                                      build();
  }
}