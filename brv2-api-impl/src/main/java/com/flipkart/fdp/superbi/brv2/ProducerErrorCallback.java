package com.flipkart.fdp.superbi.brv2;

import com.flipkart.fdp.compito.api.clients.producer.ProducerCallback;
import com.flipkart.fdp.compito.api.clients.producer.ProducerRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerErrorCallback implements ProducerCallback<String, SuperBiMessage> {

  @Override
  public void onCompletion(ProducerRecord<String, SuperBiMessage> producerRecord,
      Exception e) {
    if (e != null) {
      log.error("Sending {} record to message Queue failed with exception.", producerRecord.value().toString(), e);
    }
  }
}
