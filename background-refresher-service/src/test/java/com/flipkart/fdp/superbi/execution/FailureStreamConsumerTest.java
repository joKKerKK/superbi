package com.flipkart.fdp.superbi.execution;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.api.cache.CacheDao;
import com.flipkart.fdp.superbi.refresher.api.config.BackgroundRefresherConfig;
import com.flipkart.fdp.superbi.refresher.api.execution.QueryPayload;
import com.flipkart.fdp.superbi.refresher.api.result.query.AttemptInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class FailureStreamConsumerTest {

  @Mock
  private CacheDao cacheDao;

  @Mock
  private BackgroundRefreshTask task;

  private MetricRegistry metricRegistry = new MetricRegistry();

  private final QueryPayload queryPayload = new QueryPayload("STORE","attempt_key","cache_key"
      , 0L,null,0L,null,null,"request_id",null,null
      ,null,null);
  private final BackgroundRefresherConfig backgroundRefresherConfig = new BackgroundRefresherConfig(0,
      0,0,0,0,0,
      0,0,0,0,0);

  @Test
  public void testWithClientSideException(){
    FailureStreamConsumer failureStreamConsumer = new FailureStreamConsumer(cacheDao,metricRegistry);
    ArgumentCaptor<AttemptInfo> captor = ArgumentCaptor
        .forClass(AttemptInfo.class);
    Mockito.doNothing().when(cacheDao).set(Mockito.anyString(),Mockito.anyInt(),captor.capture());
    Mockito.when(task.getQueryPayload()).thenReturn(queryPayload);
    Mockito.doNothing().when(task).unLockTask();
    Mockito.when(task.getBackgroundRefresherConfig()).thenReturn(backgroundRefresherConfig);
    Mockito.when(task.getRemainingRetry()).thenReturn(2);
    failureStreamConsumer.accept(task,new ClientSideException("message"));
    Assert.assertEquals(captor.getValue().isServerError(),false);
    Mockito.verify(task,Mockito.times(0)).submitForRetry();
  }

  @Test
  public void testWithRetryExhaust(){
    FailureStreamConsumer failureStreamConsumer = new FailureStreamConsumer(cacheDao,metricRegistry);
    ArgumentCaptor<AttemptInfo> captor = ArgumentCaptor
        .forClass(AttemptInfo.class);
    Mockito.doNothing().when(cacheDao).set(Mockito.anyString(),Mockito.anyInt(),captor.capture());
    Mockito.when(task.getQueryPayload()).thenReturn(queryPayload);
    Mockito.doNothing().when(task).unLockTask();
    Mockito.when(task.getBackgroundRefresherConfig()).thenReturn(backgroundRefresherConfig);
    Mockito.when(task.getRemainingRetry()).thenReturn(0);
    failureStreamConsumer.accept(task,new ServerSideException(new RuntimeException()));
    Assert.assertEquals(captor.getValue().isServerError(),false);
    Mockito.verify(task,Mockito.times(0)).submitForRetry();
  }

  @Test
  public void testWithRetryRemainingAndServerSide(){
    FailureStreamConsumer failureStreamConsumer = new FailureStreamConsumer(cacheDao,metricRegistry);
    ArgumentCaptor<AttemptInfo> captor = ArgumentCaptor
        .forClass(AttemptInfo.class);
    Mockito.doNothing().when(cacheDao).set(Mockito.anyString(),Mockito.anyInt(),captor.capture());
    Mockito.when(task.getQueryPayload()).thenReturn(queryPayload);
    Mockito.doNothing().when(task).unLockTask();
    Mockito.when(task.getBackgroundRefresherConfig()).thenReturn(backgroundRefresherConfig);
    Mockito.when(task.getRemainingRetry()).thenReturn(2);
    failureStreamConsumer.accept(task,new ServerSideException(new RuntimeException()));
    Assert.assertEquals(captor.getValue().isServerError(),true);
    Mockito.verify(task,Mockito.times(1)).submitForRetry();
  }
}
