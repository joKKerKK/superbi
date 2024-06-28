package com.flipkart.fdp.superbi.brv2;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.flipkart.fdp.compito.api.clients.producer.Producer;
import com.flipkart.fdp.compito.api.clients.producer.ProducerCallback;
import com.flipkart.fdp.compito.api.clients.producer.ProducerRecord;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackgroundQuerySubmitterTest extends BackgroundRefresherTestSuite {

  @Mock
  private Producer<String, SuperBiMessage> producer;

  @Mock
  private ProducerCallback<String, SuperBiMessage> producerCallback;

  @Captor
  private ArgumentCaptor<ProducerRecord<String, SuperBiMessage>> captor;

  @Test
  public void testBRv2Producer() {
    BackgroundQuerySubmitter backgroundQuerySubmitter = new BackgroundQuerySubmitter(producer,
        producerCallback, this::getBackgroundRefresherConfig, this::generateTopicName, MessageQueue.PUBSUB_LITE);
    backgroundQuerySubmitter.submitQuery(queryPayload);
    verify(producer, times(1)).send(captor.capture(), anyObject());
    ProducerRecord<String, SuperBiMessage> producerRecord = captor.getValue();
    Assert.assertEquals(TEST_CACHE_KEY, producerRecord.key());
    SuperBiMessage retryableMessage = producerRecord.value();
    Assert.assertEquals(3, retryableMessage.getRemainingRetries());
    Assert.assertEquals(1000, retryableMessage.getBackoffInMillis());
    Assert.assertEquals(queryPayload, retryableMessage.getQueryPayload());
  }

  private BackgroundRefresherConfig getBackgroundRefresherConfig(String storeIdentifier) {
    return new BackgroundRefresherConfig(
        2, 3, 4, 5, 6, 7, 3, 1000, 2, 0.0d
    ,1024);
  }

  private String generateTopicName(QueryPayload queryPayload) {
    return queryPayload.getStoreIdentifier().concat("-").concat(queryPayload.getPriority());
  }
}
