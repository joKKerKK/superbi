package com.flipkart.fdp.superbi.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import java.io.IOException;

public class NativeQuerySerializer extends StdSerializer<NativeQuery> {

  public NativeQuerySerializer(
      Class<NativeQuery> t) {
    super(t);
  }

  public NativeQuerySerializer() {
    this(null);
  }

  @Override
  public void serialize(NativeQuery nativeQuery, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("classType", nativeQuery.getNativeQueryClassName());
    jsonGenerator.writeStringField("value", JsonUtil.toJson(nativeQuery.getQuery()));
    jsonGenerator.writeEndObject();
  }
}
