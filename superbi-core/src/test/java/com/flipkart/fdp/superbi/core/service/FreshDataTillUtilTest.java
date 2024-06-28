package com.flipkart.fdp.superbi.core.service;


import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class FreshDataTillUtilTest {

  @Test
  public void testFactRefreshTimeBeforeCache(){

    final long currentTime = new Date().getTime();
    final long factRefreshedAtTime = currentTime - 10000;
    final long resultCacheTime = currentTime - 10;
    long freshAsOf = DataService.getFreshAsOf(currentTime,factRefreshedAtTime,resultCacheTime);
    Assert.assertEquals(freshAsOf,currentTime);
  }

  @Test
  public void testFactRefreshTimeAfterCache(){
    final long currentTime = new Date().getTime();
    final long resultCacheTime = currentTime - 10000;
    final long factRefreshedAtTime = currentTime - 10;
    long freshAsOf = DataService.getFreshAsOf(currentTime, factRefreshedAtTime,
        resultCacheTime);
    Assert.assertEquals(freshAsOf, resultCacheTime);
  }

  @Test
  public void testFactRefreshTimeForElasticSearch(){
    final long currentTime = new Date().getTime();
    final long resultCacheTime = currentTime - 10000;
    final long factRefreshedAtTime = 0;
    long freshAsOf = DataService.getFreshAsOf(currentTime,factRefreshedAtTime,currentTime-10000);
    Assert.assertEquals(freshAsOf,resultCacheTime);
  }

}
