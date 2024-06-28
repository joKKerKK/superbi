package com.flipkart.fdp.superbi.utils;

import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.flipkart.fdp.superbi.models.NativeQuery;
import com.flipkart.fdp.superbi.serde.NativeQueryDeserializer;
import com.flipkart.fdp.superbi.serde.NativeQuerySerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * Created by akshaya.sharma on 06/09/19
 */

public class ObjectMapperUtils {

  public static void configure(ObjectMapper objectMapper) {
    objectMapper.registerModule(new GuavaModule());
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, false));
    objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd"));
    objectMapper.setSerializationInclusion(Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    SimpleModule module = new SimpleModule();
    module.addSerializer(Number.class, new NumberSerializer());
    module.addSerializer(NativeQuery.class, new NativeQuerySerializer());
    module.addDeserializer(NativeQuery.class, new NativeQueryDeserializer());
    objectMapper.registerModule(module);

  }

  static class NumberSerializer extends com.fasterxml.jackson.databind.ser.std.NumberSerializer {

    NumberSerializer() {
      super(Number.class);
    }

    @Override
    public void serialize(Number value, JsonGenerator g,
        SerializerProvider provider) throws IOException {
      if (value instanceof BigDecimal) {
        value = new Double(value.doubleValue());
      }

      if (value instanceof Double) {
        if (Double.isNaN((Double) value) || Double.isInfinite((Double) value)) {
          g.writeNumber("\"NaN\"");
          return;
        }
        String realString = value.toString();
        g.writeNumber(realString);
      } else {
        super.serialize(value, g, provider);
      }
    }
  }
}
