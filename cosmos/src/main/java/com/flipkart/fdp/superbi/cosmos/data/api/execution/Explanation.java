package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import static com.flipkart.fdp.superbi.cosmos.data.api.execution.ExecutorStore.EXECUTOR_STORE;

import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.hystrix.ActualCall;
import com.flipkart.fdp.superbi.cosmos.hystrix.RemoteCall;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Source;
import com.flipkart.fdp.superbi.cosmos.utils.Constants;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by amruth.s on 27-10-2014.
 */
public class Explanation {
  private static final Logger logger = LoggerFactory.getLogger(Explanation.class);

  public final Object nativeQuery;
  public final Object nativeQueryExplanation;

  public Explanation(Object nativeQuery, Object nativeQueryExplanation) {
    this.nativeQuery = nativeQuery;
    this.nativeQueryExplanation = nativeQueryExplanation;
  }

  public static Explanation getFor(final DSQuery query, final Map<String, String[]> params) {
    return getFor(query, Source.FederationType.DEFAULT, params);
  }

  public static Explanation getFor(final DSQuery query, final Source.FederationType federationType, final Map<String, String[]> params) {
    final Object nativeQuery = getNativeQueryOrErrorMessage(query, params);
    final Object nativeQueryExplanation = getNativeQueryExpOrErrMsg(query, params, nativeQuery, federationType);
    return new Explanation(
        nativeQuery,
        nativeQueryExplanation);
  }

  private static Object getNativeQueryExpOrErrMsg(final DSQuery query,
      final Map<String, String[]> params,
      final Object nativeQuery) {
    return getNativeQueryExpOrErrMsg(query, params, nativeQuery, Source.FederationType.DEFAULT);
  }

  private static Object getNativeQueryExpOrErrMsg(final DSQuery query,
      final Map<String, String[]> params,
      final Object nativeQuery, Source.FederationType federationType) {
    final AbstractDSLConfig config = ExecutorFacade.instance.getConfigFor(query, federationType, params);
    try {
      return new RemoteCall.Builder<Object>(EXECUTOR_STORE.getSourceFor(query))
          .withTimeOut((int) config.getQueryTimeOutMs())
          .around(new ActualCall<Object>() {
            @Override
            public Object workUnit() {
              return EXECUTOR_STORE.getFor(query, federationType).explainNative(nativeQuery);
            }
          }).execute();
    } catch (Exception e) {
      logger.error("Failed explanation", e);
      return extractErrorMessage(e);
    }
  }

  private static Object extractErrorMessage(Exception e) {
    String message = ExceptionUtils.getRootCause(e).getMessage();
    if (message.contains("SemanticException")) {
      String[] tokens = message.split("SemanticException");
      if (tokens.length > 1)
        message = tokens[1];
    }
    return Constants.ERROR + " " + message;
  }

  public static Object getNativeQueryOrErrorMessage(final DSQuery query, final Map<String, String[]> params) {
    try {
      return ExecutorFacade.instance.getNativeQuery(query, params);
    } catch (Exception e) {
      logger.error("Query translate failed", e);
      return "Query translate failed, Please report the issue";
    }

  }
}
