package com.flipkart.fdp.superbi.refresher.dao.elastic;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.exceptions.SuperBiException;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by akshaya.sharma on 03/04/20
 */

public class ElasticSearchExceptionMap {
  private List<String> internalList = null;

  /**
   * ElasticSearchExceptions are declared at https://github.com/elastic/elasticsearch/blob/v7.4
   * .0/server/src/main/java/org/elasticsearch/ElasticsearchException.java#L737-L1030
   * We must be able to transpose these into ClientSideException or ServerSideException
   * We are taking a conscious call to treat all as ServerSideException until explicitly declared
   * as ClientSideException
   */
  public static final List<String> CLIENT_SIDE_EXCEPTIONS = Lists.newArrayList(
      "INDEX_NOT_FOUND_EXCEPTION", "GENERAL_SCRIPT_EXCEPTION",
      "DOCUMENT_MISSING_EXCEPTION", "ELASTICSEARCH_PARSE_EXCEPTION", "INDEX_CLOSED_EXCEPTION",
      "RESOURCE_NOT_FOUND_EXCEPTION", "INVALID_TYPE_NAME_EXCEPTION",
      "INDEX_TEMPLATE_MISSING_EXCEPTION", "AGGREGATION_EXECUTION_EXCEPTION",
      "INVALID_INDEX_TEMPLATE_EXCEPTION", "AGGREGATION_INITIALIZATION_EXCEPTION",
      "DOCUMENT_SOURCE_MISSING_EXCEPTION", "QUERY_PHASE_EXECUTION_EXCEPTION",
      "INVALID_AGGREGATION_PATH_EXCEPTION", "RESOURCE_ALREADY_EXISTS_EXCEPTION",
      "CIRCUIT_BREAKING_EXCEPTION", "TYPE_MISSING_EXCEPTION", "SCRIPT_EXCEPTION",
      "TOO_MANY_BUCKETS_EXCEPTION", "PARSING_EXCEPTION"
  );


  ElasticSearchExceptionMap() {
    internalList = CLIENT_SIDE_EXCEPTIONS.stream().distinct().map(k -> prepeareKey(k)).collect(
        Collectors.toList());
  }

  /**
   * This is the way elastic search transform Exceptions into a string property called "type" or
   * its error response JSON
   * With this way we can be aligned with ES standards and keep increasing out exception list
   * CLIENT_SIDE_EXCEPTIONS
   * This method "prepeareKey" is copied from
   * https://github.com/elastic/elasticsearch/blob/v7.4
   * .0/server/src/main/java/org/elasticsearch/ElasticsearchException.java#L654-L661
   */
  private static String prepeareKey(String simpleName) {
    if (simpleName.startsWith("ELASTICSEARCH_")) {
      simpleName = simpleName.substring("ELASTICSEARCH_".length());
    }
    // TODO: do we really need to make the exception name in underscore casing?
    return toUnderscoreCase(simpleName);
  }

  /**
   * This method "toUnderscoreCase" is copied from
   * https://github.com/elastic/elasticsearch/blob/v7.4
   * .0/server/src/main/java/org/elasticsearch/ElasticsearchException.java#L1137-L1169
   */
  private static String toUnderscoreCase(String value) {
    StringBuilder sb = new StringBuilder();
    boolean changed = false;
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (Character.isUpperCase(c)) {
        if (!changed) {
          // copy it over here
          for (int j = 0; j < i; j++) {
            sb.append(value.charAt(j));
          }
          changed = true;
          if (i == 0) {
            sb.append(Character.toLowerCase(c));
          } else {
            sb.append('_');
            sb.append(Character.toLowerCase(c));
          }
        } else {
          sb.append('_');
          sb.append(Character.toLowerCase(c));
        }
      } else {
        if (changed) {
          sb.append(c);
        }
      }
    }
    if (!changed) {
      return value;
    }
    return sb.toString();
  }

  // Retrun ServerSideException.class if not defined
  public Class<? extends SuperBiException> get(String key) {
    key = key == null ? StringUtils.EMPTY : key;
    key = prepeareKey(key.toUpperCase());
    return internalList.contains(key) ? ClientSideException.class : ServerSideException.class;
  }

  public SuperBiException buildException(String key, String message) {
    return buildException(key, message, null);
  }

  @SneakyThrows
  public SuperBiException buildException(String key, String message, Throwable t) {
    Class<? extends SuperBiException> exceptionClass = get(key);
    return exceptionClass.getConstructor(String.class, Throwable.class).newInstance(message, t);
  }
}
