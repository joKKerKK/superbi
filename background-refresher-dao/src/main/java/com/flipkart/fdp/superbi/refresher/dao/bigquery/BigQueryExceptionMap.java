package com.flipkart.fdp.superbi.refresher.dao.bigquery;

import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.exceptions.SuperBiException;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by mansi.jain on 22/02/22
 */
public class BigQueryExceptionMap {

  private List<String> internalList = null;

  /**
   * BigQueryErrorCodes are declared at https://cloud.google.com/bigquery/docs/error-messages We
   * must be able to transpose these into ClientSideException or ServerSideException. We are taking
   * a conscious call to treat all as ClientSideException until explicitly declared as
   * ServerSideException.
   */
  public static final List<String> SERVER_SIDE_EXCEPTIONS = Lists.newArrayList(
      "backendError", "blocked", "internalError", "notImplemented"
  );

  BigQueryExceptionMap() {
    internalList = SERVER_SIDE_EXCEPTIONS;
  }

  public Class<? extends SuperBiException> get(String key) {
    key = key == null ? StringUtils.EMPTY : key;
    return !internalList.contains(key) ? ClientSideException.class : ServerSideException.class;
  }

  @SneakyThrows
  public SuperBiException buildException(String key, String message, Throwable t) {
    Class<? extends SuperBiException> exceptionClass = get(key);
    return exceptionClass.getConstructor(String.class, Throwable.class).newInstance(message, t);
  }

}
