package com.flipkart.fdp.superbi.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.io.IOException;
import lombok.SneakyThrows;

public class NativeQueryDeserializer  extends StdDeserializer<NativeQuery> {
  public NativeQueryDeserializer(Class<?> vc) {
    super(vc);
  }

  public NativeQueryDeserializer() {
    this(null);
  }

  @Override
  @SneakyThrows
  public NativeQuery deserialize(JsonParser jsonParser,
      DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    Class classType = Class.forName(node.get("classType").asText());
    String value = node.get("value").asText();

    return new NativeQuery(JsonUtil.fromJson(value, classType));
  }
}
