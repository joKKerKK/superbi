package com.flipkart.fdp.superbi.refresher.dao.hdfs;

import com.flipkart.fdp.superbi.http.client.exceptions.ApiException;
import com.flipkart.fdp.superbi.http.client.gringotts.GringottsClient;
import com.flipkart.fdp.superbi.http.client.ironbank.IronBankClient;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.dao.DataSourceDao;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.JdbcQueryResult;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.service.rpc.thrift.TOperationHandle;

@Slf4j
public class HdfsDataSourceDao implements DataSourceDao {

  public static final String ADHOC_QUEUE_POS = "3";
  public static final String PRIORITY_TO_QUEUE_NAME_MAP_KEY = "priority_to_queue_name_map";
  public static final String SET_MAPRED_JOB_NAME_QUERY = "set mapred.job.name=";
  public static final String SET_HIVE_QUERY_NAME_QUERY = "set hive.query.name=";

  public HdfsDataSourceDao(CacheDao handleStore,HdfsConfig hdfsConfig,GringottsClient gringottsClient,
      IronBankClient ironBankClient) {
    this.handleStore = handleStore;
    this.hdfsConfig = hdfsConfig;
    this.gringottsClient = gringottsClient;
    this.ironBankClient = ironBankClient;
  }

  private final CacheDao handleStore;
  private final HdfsConfig hdfsConfig;
  private final GringottsClient gringottsClient;
  private final IronBankClient ironBankClient;


  @Override
  public QueryResult getStream(DataSourceQuery dataSourceQuery) {
    Optional<HiveDriverHandle> driverHandleOptional = Optional.empty();
    String handleKey = getHandleKey(dataSourceQuery.getCacheKey());
    try {
      driverHandleOptional = handleStore.get(handleKey,HiveDriverHandle.class);
      Connection connection;
      HiveStatement statement;
      if(driverHandleOptional.isPresent()){
        log.info(MessageFormat.format("Latching for handleKey {0}",handleKey));
        connection = getConnection(driverHandleOptional.get().getHost(),driverHandleOptional.get().getUser()
            , hdfsConfig.getPassword());
        statement = (HiveStatement) connection.createStatement();

        statement.latchAsync(driverHandleOptional.get().getAsTOperationHandle());
        log.info(MessageFormat.format("Latching successful for handleKey {0}",handleKey));

      }else{
        log.info(MessageFormat.format("New connection  for handleKey {0}",handleKey));
        connection = getConnection(hdfsConfig.getJdbcUrl(), hdfsConfig.getUsername(), hdfsConfig.getPassword());
        statement = (HiveStatement) connection.createStatement();
        HiveDriverHandle hiveDriverHandle = startNewExecution(dataSourceQuery,statement,handleKey);
        handleStore.set(handleKey, hdfsConfig.getRecoveryTimeOutLimitMs() / 1000,hiveDriverHandle);
      }
      log.info(MessageFormat.format("Polling and getting result for handleKey {0}",handleKey));
      final QueryResult queryResult = pollAndGetResult(statement,connection);
      log.info(MessageFormat.format("Result fetched for handleKey {0}",handleKey));
      handleStore.remove(handleKey);
      return queryResult;
    } catch (Exception e) {
      if(driverHandleOptional.isPresent()){
        handleStore.remove(handleKey);
      }
      log.warn("Execution failed for {} due to {}", dataSourceQuery.getNativeQuery(), e.getMessage());
      throw new ServerSideException(e);
    }
  }

  @SneakyThrows
  public HiveDriverHandle startNewExecution(DataSourceQuery dataSourceQuery,HiveStatement statement
      ,String handleKey) {
    log.info("Starting new execution");
    String queue = hdfsConfig.getQueue();
    String username = hdfsConfig.getUsername();
    if(!hdfsConfig.getPriorityClient().equals(dataSourceQuery.getMetaDataPayload().getClient())){
      queue = getQueueForQueryExecution(dataSourceQuery, handleKey);
    }
    final String hiveServerURL = ((HiveConnection) (statement.getConnection())).getConnectedUrl();
    log.info(MessageFormat.format("Got connection resolver for handleKey {0}",handleKey));
    String queryString = String.valueOf(dataSourceQuery.getNativeQuery());

    prepareStatement(statement, handleKey, queue, username);
    log.info("Submitting query to hive server {}", hiveServerURL);
    guardedExecuteAsync(statement, queryString);

    return getHiveDriverHandleFromStatement(statement, username, hiveServerURL);
  }

  private HiveDriverHandle getHiveDriverHandleFromStatement(HiveStatement statement,
      String username, String hiveServerURL) {
    final TOperationHandle tOperationHandle = statement.getOperationHandle();
    log.info(" TOperationHandle - {}", tOperationHandle);
    final byte[] secret = tOperationHandle.getOperationId().bufferForSecret().array();
    final byte[] guid = tOperationHandle.getOperationId().bufferForGuid().array();
    final String zookeeperNamespace = extractZKNamespace(hiveServerURL);
    return new HiveDriverHandle(
        secret,
        guid,
        hiveServerURL
            .replace(";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=" + zookeeperNamespace, ""),
        10000,
        username
    );
  }

  private void prepareStatement(HiveStatement statement, String handleKey, String queue,
      String username) {
    initConnection(statement);

    guardedExecute(statement, SET_MAPRED_JOB_NAME_QUERY + handleKey + "-" + username);
    guardedExecute(statement, SET_HIVE_QUERY_NAME_QUERY + handleKey + "-" + username);

    setContextualProperties(queue,statement);
  }

  private String getQueueForQueryExecution(DataSourceQuery dataSourceQuery, String handleKey) {
    String queue;
    try{
      List<String> billingOrg = gringottsClient.getBillingOrg(dataSourceQuery.getMetaDataPayload().getUsername());
      if(billingOrg!= null && !billingOrg.isEmpty()){
        Map<String,Object> ironBankResponse = ironBankClient.getQueue(billingOrg.get(0));
        queue  = ((Map<String,String>)ironBankResponse.get(PRIORITY_TO_QUEUE_NAME_MAP_KEY)).get(
            ADHOC_QUEUE_POS);
      }else{
        throw new ApiException(MessageFormat.format("Billing org not found for user {0}",dataSourceQuery.getMetaDataPayload().getUsername()));
      }
      log.info(MessageFormat.format("Got billingOrg {0} for user {1} for handleKey {2}",billingOrg.get(0)
          ,dataSourceQuery.getMetaDataPayload().getUsername(),handleKey));
    }catch (Exception e){
      log.info(MessageFormat.format("API Execution failed for handleKey {0}", handleKey));
      throw new ApiException(e);
    }
    return queue;
  }

  private static String extractZKNamespace(String jdbcURL) {
    Pattern pattern = Pattern.compile(";.*zooKeeperNamespace=(.*)\\?");
    Matcher matcher = pattern.matcher(jdbcURL);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return null;
    }
  }

  private void setContextualProperties(String queue,HiveStatement statement) {
    executeSetQuery("set mapreduce.job.priority","0",statement);
    executeSetQuery("set tez.queue.name",queue,statement);
    executeSetQuery("set mapreduce.job.queuename",queue,statement);

  }

  private void executeSetQuery(String property,String val,HiveStatement statement){
    guardedExecute(statement, property + "=" + val);
  }

  private void initConnection(HiveStatement statement) {
    for (String initQ : hdfsConfig.getInitScripts()) {
      guardedExecute(statement, initQ);
    }
  }

  @SneakyThrows
  private Connection getConnection(String url,String user,String password) {
    DriverManager.setLoginTimeout(10000);
    Connection connection = DriverManager.getConnection(url,user,password);
    return connection;
  }

  @SneakyThrows
  public QueryResult pollAndGetResult(HiveStatement statement,Connection connection)  {
    guardedWait(statement);
    return buildResultFor(statement,connection);

  }

  @SneakyThrows
  protected QueryResult buildResultFor(HiveStatement statement,Connection connection) {
    return new HiveQueryResult(statement.getResultSet(),connection);

  }

  private void guardedExecuteAsync(HiveStatement statement, String queryString) {

    hiveTOperation(() -> statement.executeAsync(queryString));
  }

  private void guardedWait(HiveStatement hiveStatement) {
    hiveTOperation(() -> {
      hiveStatement.getUpdateCount();
      return null;
    });
  }


  private void guardedExecute(HiveStatement statement, String queryString) {
    hiveTOperation(() -> statement.execute(queryString));
  }

  interface HiveOpr<T> {
    T doOpr() throws SQLException;
  }
  private enum IterationState { BEFORE_START, DURING, AFTER_END}
  @SneakyThrows
  private <T> void hiveTOperation(HiveOpr<T> hiveOpr) {
    hiveOpr.doOpr();
  }

  private class HiveQueryResult extends JdbcQueryResult {

    private final Connection connection;

    @Override
    public void close() {
      try{
        if(!connection.isClosed()){
          connection.close();
        }
      }catch (Exception e){
        log.error("Not able to cloase connection due to",e);
      }
    }

    @SneakyThrows
    private HiveQueryResult(ResultSet resultSet,Connection connection) {
      this.resultSet = resultSet;
      this.connection = connection;
      try {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i < columnCount + 1; i++ ) {
          this.schema.add(metaData.getColumnLabel(i));
        }
      } catch (SQLException e) {
        close();
        throw e;
      }
    }
  }
  private String getHandleKey(String cacheKey){
    return new StringBuilder(cacheKey).append("_handle").toString();
  }
}