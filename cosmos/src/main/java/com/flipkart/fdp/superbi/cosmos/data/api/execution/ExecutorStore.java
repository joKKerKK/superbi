package com.flipkart.fdp.superbi.cosmos.data.api.execution;

import com.flipkart.fdp.superbi.cosmos.data.hive.HiveDSQueryExecutor;
import com.flipkart.fdp.superbi.dsl.query.DSQuery;
import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.Source;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.View;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 19/08/14
 */

public class ExecutorStore {

  private static final Logger logger = LoggerFactory.getLogger(ExecutorStore.class);

  private Map<String, Map< com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType,
      DSQueryExecutor>> executorStore;
  public static final ExecutorStore EXECUTOR_STORE = new ExecutorStore();

  private ExecutorStore() {
    executorStore = Maps.newHashMap();
  }

  public void initializeAll() {
    for (View.Source source : MetaAccessor.get().getSources()) {
      EXECUTOR_STORE.addFor(source.getName(), source.getFederationType());
    }
  }

  public DSQueryExecutor getFor(String sourceName) {
    return getFor(sourceName,
         com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType.DEFAULT);
  }

  public DSQueryExecutor getFor(String sourceName,
       com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType federationType) {
    Map< com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType, DSQueryExecutor>
        federatedExecutors;
    if (executorStore.containsKey(sourceName)) {
      federatedExecutors = executorStore.get(sourceName);
      if (federatedExecutors.containsKey(federationType)) {
        return federatedExecutors.get(federationType);
      }
    }
    return addFor(sourceName, federationType);
  }

  private DSQueryExecutor addFor(String sourceName,
       com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType federationType) {
    final MetaAccessor accessor = MetaAccessor.get();
    Source source = null;
    try {
      source = accessor.getSourceByName(sourceName, federationType);
    }catch (RuntimeException ex) {
      // could not find the desired federated source
      if(federationType !=  com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType.DEFAULT) {
        // Desired Federation is not registered, Force redirect to DEFAULT
        logger.info("Fedration: {} is not found for source: {}, Delegating to DEFAULT", new Object[] {federationType, sourceName});
        federationType =  com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType.DEFAULT;
        source = accessor.getSourceByName(sourceName, federationType);
      }else {
        throw ex;
      }
    }

    if (!executorStore.containsKey(sourceName)) {
      executorStore.put(sourceName, Maps.newConcurrentMap());
    }

    Map< com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType, DSQueryExecutor>
        federatedExecutors = executorStore.get(
        sourceName);

    if (federatedExecutors.containsKey(federationType)) {
      return federatedExecutors.get(federationType);
    }

    final DSQueryExecutor executor = ExecutorType.initializeExecutor(source);
    federatedExecutors.put(federationType, executor);

    return executor;
  }

  public DSQueryExecutor getFor(DSQuery query) {
    return getFor(query,  com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType.DEFAULT);
  }

  public DSQueryExecutor getFor(DSQuery query,
       com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType federationType) {
    return getFor(getSourceFor(query), federationType);
  }

  public void invalidateAll() {
    executorStore = Maps.newHashMap();
  }

  public void invalidateFor(String sourceName) {
    if (!executorStore.containsKey(sourceName)) {
      throw new UnsupportedOperationException("Source is not registered: " + sourceName);
    }
    executorStore.remove(sourceName);
  }

  public String getSourceFor(DSQuery query) {
    final MetaAccessor accessor = MetaAccessor.get();
    final String entityName = query.getFromTable();
    View.Entity dataSourceObj = accessor.getEntityBy(entityName);
    Preconditions.checkNotNull(dataSourceObj, "We don't have this table in the store!");
    return dataSourceObj.getTableSource();
  }

  public HiveDSQueryExecutor getHiveExecutor(
       com.flipkart.fdp.superbi.cosmos.meta.model.data.Source.FederationType federationType) {
    String hdfsSourceName = MetaAccessor.get().getSources().stream().filter(
        s -> s.getSourceType().equals(
            SourceType.HDFS.name())).findFirst().get().getName();
    return (HiveDSQueryExecutor) getFor(hdfsSourceName, federationType);
  }
}
