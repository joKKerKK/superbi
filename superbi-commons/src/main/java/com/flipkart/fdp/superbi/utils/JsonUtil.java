package com.flipkart.fdp.superbi.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsonschema.JsonSchema;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.flipkart.fdp.superbi.exceptions.JsonDeserializationException;
import com.flipkart.fdp.superbi.exceptions.UnCheckedSerializationException;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA. User: vishnuhr Date: 30/12/12 Time: 2:37 PM Utility for converting json
 * string to pojo. This utility uses 'FasterXML / jackson-databind' @
 * https://github.com/FasterXML/jackson-databind
 */

public class JsonUtil {

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    ObjectMapperUtils.configure(mapper);
  }

  public static <T> T fromJson(String json_string, Class<T> klass) {
    try {
      return mapper.readValue(json_string, TypeFactory.defaultInstance().constructType(klass));
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static <T> T fromJson(InputStream jsonInputStream, Class<T> klass) {
    try {
      return mapper.readValue(jsonInputStream, TypeFactory.defaultInstance().constructType(klass));
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static <T> T fromJson(String json_string, TypeReference<T> klass) {
    try {
      return mapper.readValue(json_string, TypeFactory.defaultInstance().constructType(klass));
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static <T> T fromJson(JsonNode node, Class<T> klass) {
    try {
      return mapper.treeToValue(node, klass);
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static <T> Optional<T> fromJsonOptional(String json_string, Class<T> klass) {

    if (Strings.isNullOrEmpty(json_string)) {
      return Optional.absent();
    } else {
      return Optional.fromNullable(fromJson(json_string, klass));
    }
  }

  public static String toJson(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static JsonNode toJsonNode(Object object) {
    return mapper.valueToTree(object);
  }

  public static String toPrettyJson(Object object) {
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

//    public static<T> void updateValue(T type, String jsonString) throws JsonProcessingException, IOException {
//        mapper.readerForUpdating(type).readValue(jsonString);
//    }

  public static JsonSchema getSchema(Class klass) {
    try {
      return mapper.generateJsonSchema(klass);
    } catch (JsonMappingException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static JsonNode toJsonNode(byte[] bytes) {
    try {
      return mapper.readTree(bytes);
    } catch (IOException e) {
      throw new UnCheckedSerializationException(e);
    }
  }

  public static <T> T convertValue(Object fromValue, Class<T> toValueType) {

    return (T) mapper.convertValue(fromValue, toValueType);
  }

  public static Long getJsonSize(Object fromValue) {
    try {
      return (long) mapper.writeValueAsBytes(fromValue).length;
    } catch (JsonProcessingException e) {
      throw new UnCheckedSerializationException(e);
    }

  }

  public static Object generateJsonFilteredObject(Object object,
      Optional<List<String>> paramsOptional, String filterName) {

    ObjectMapper mapper = new ObjectMapper();
    FilterProvider filters;

    if (paramsOptional.isPresent()) {
      filters = new SimpleFilterProvider().addFilter(filterName,
          SimpleBeanPropertyFilter.filterOutAllExcept(new HashSet<String>(paramsOptional.get())));
    } else {
      filters = new SimpleFilterProvider().addFilter(filterName,
          SimpleBeanPropertyFilter.serializeAllExcept(Collections.<String>emptySet()));
    }

    Object filteredObject = null;

    try {
      mapper.setFilters(filters);
      filteredObject = mapper.convertValue(object, Object.class);
    } catch (IllegalArgumentException ex) {
      throw new UnCheckedSerializationException(ex);
    }

    return filteredObject;
  }

  public static Map convertJsontoMap(JsonNode node) {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, ?> jsonMap = null;

    try {
      jsonMap = mapper.convertValue(node, Map.class);

    } catch (IllegalArgumentException ex) {
      throw new JsonDeserializationException(ex);
    }
    return jsonMap;
  }


  public static ObjectNode convertMessageToJson(String message) {
    ObjectNode node = mapper.createObjectNode();
    node.put("message", message);
    return node;
  }
}