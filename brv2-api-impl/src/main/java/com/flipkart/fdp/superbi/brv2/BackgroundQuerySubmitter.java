package com.flipkart.fdp.superbi.brv2;

import com.flipkart.fdp.compito.api.clients.producer.Producer;
import com.flipkart.fdp.compito.api.clients.producer.ProducerCallback;
import com.flipkart.fdp.compito.api.clients.producer.ProducerRecord;
import com.flipkart.fdp.compito.kafka.KafkaProducerRecord;
import com.flipkart.fdp.compito.pubsublite.PubsubLiteProducerRecord;
import com.flipkart.fdp.superbi.models.MessageQueue;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.BackgroundRefresher;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import java.text.MessageFormat;
import java.util.function.Function;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BackgroundQuerySubmitter implements BackgroundRefresher {

  private final Producer<String, SuperBiMessage> producer;
  private final ProducerCallback<String, SuperBiMessage> producerCallback;
  private final Function<String, BackgroundRefresherConfig> backgroundRefresherConfigProvider;
  private final Function<QueryPayload, String> generateTopicName;
  private final MessageQueue messageQueue;

  @Override
  public void submitQuery(QueryPayload queryPayload) {
    BackgroundRefresherConfig backgroundRefresherConfig = backgroundRefresherConfigProvider
        .apply(queryPayload.getStoreIdentifier());
    if(backgroundRefresherConfig == null) {
        throw new UnsupportedOperationException(
            MessageFormat.format("DataSource invalid for {0} ", queryPayload.getStoreIdentifier()));
    }
    String topicName = generateTopicName.apply(queryPayload);
    String key = queryPayload.getCacheKey();
    int numberOfRetry = backgroundRefresherConfig.getNumOfRetryOnException();
    long backoffInMillis = backgroundRefresherConfig.getRetryOnExceptionBackoffInMillis();
    SuperBiMessage superBiMessage = new SuperBiMessage(numberOfRetry, System.currentTimeMillis(),
        backoffInMillis, queryPayload);
    ProducerRecord<String,SuperBiMessage> producerRecord;
    if ( messageQueue == MessageQueue.PUBSUB_LITE ) {
      producerRecord = new PubsubLiteProducerRecord<>(key, superBiMessage, topicName);
    } else {
      producerRecord = new KafkaProducerRecord<>(key, superBiMessage, topicName);
    }
    producer.send(producerRecord, producerCallback);
  }
}
