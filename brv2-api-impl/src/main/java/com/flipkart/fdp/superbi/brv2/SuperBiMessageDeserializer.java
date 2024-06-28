package com.flipkart.fdp.superbi.brv2;

import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class SuperBiMessageDeserializer implements
    Deserializer<SuperBiMessage> {

  private String encoding = "UTF8";

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) {
      encodingValue = configs.get("deserializer.encoding");
    }
    if (encodingValue instanceof String) {
      encoding = (String) encodingValue;
    }
  }

  @Override
  public SuperBiMessage deserialize(String topic, byte[] data) {
    try {
      if (data == null) {
        return null;
      } else {
        String jsonString = new String(data, encoding);
        return JsonUtil.fromJson(jsonString, SuperBiMessage.class);
      }
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing byte[] to SuperBiMessage.", e);
    }
  }

  @Override
  public void close() {
    //Do nothing.
  }
}
