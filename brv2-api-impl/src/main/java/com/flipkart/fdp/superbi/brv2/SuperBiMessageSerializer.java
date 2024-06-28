package com.flipkart.fdp.superbi.brv2;

import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class SuperBiMessageSerializer implements Serializer<SuperBiMessage> {

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
  public byte[] serialize(String topic, SuperBiMessage data) {
    try {
      if (data == null) {
        return null;
      } else {
        String jsonString = JsonUtil.toJson(data);
        return jsonString.getBytes(encoding);
      }
    } catch (UnsupportedEncodingException e) {
      throw new SerializationException("Error when serializing SuperBiMessage to byte[].", e);
    }
  }

  @Override
  public void close() {
    //Nothing to do.
  }
}
