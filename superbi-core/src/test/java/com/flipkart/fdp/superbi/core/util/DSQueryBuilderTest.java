package com.flipkart.fdp.superbi.core.util;

import com.flipkart.fdp.superbi.core.config.DataPrivilege.LimitPriority;
import com.google.common.base.Optional;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DSQueryBuilderTest {

  final Optional<Integer> clientLimit = Optional.of(10);
  final Optional<Integer> reportLimit = Optional.of(100);
  final Optional<Integer> configLimit = Optional.of(100000);
  final Optional<Integer> configLimitFull = Optional.of(-1);
  final DSQueryBuilder dsQueryBuilder = DSQueryBuilder.getFor(null,Optional.absent(),null, 100,
      null);

  @Test
  public void testClientCase(){
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.CLIENT),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.CLIENT),configLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.CLIENT),reportLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimit,
        Optional.absent(), LimitPriority.CLIENT),reportLimit);

    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.CLIENT),clientLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.CLIENT),clientLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.CLIENT),clientLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimit,
        Optional.absent(), LimitPriority.CLIENT),clientLimit);
  }

  @Test
  public void testReportCase(){
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.REPORT),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.REPORT),configLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.REPORT),reportLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimit,
        Optional.absent(), LimitPriority.REPORT),reportLimit);

    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.REPORT),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.REPORT),configLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.REPORT),reportLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimit,
        Optional.absent(), LimitPriority.REPORT),reportLimit);
  }

  @Test
  public void testConfigCase(){
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.CONFIG),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.CONFIG),configLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.CONFIG),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimit,
        Optional.absent(), LimitPriority.CONFIG),configLimit);

    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.CONFIG),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.CONFIG),configLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.CONFIG),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimit,
        Optional.absent(), LimitPriority.CONFIG),configLimit);

  }

  @Test
  public void testMinCase(){

    Optional<Integer> reportLimitMin = Optional.of(1);
    Optional<Integer> configLimitMin = Optional.of(1);

    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.MIN),Optional.absent());
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.MIN),configLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.MIN),reportLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(Optional.absent(),reportLimit,configLimit,
        Optional.absent(), LimitPriority.MIN),reportLimit);

    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimitFull,
        Optional.absent(), LimitPriority.MIN),clientLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,Optional.absent(),configLimit,
        Optional.absent(), LimitPriority.MIN),clientLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimitFull,
        Optional.absent(), LimitPriority.MIN),clientLimit);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimit,
        Optional.absent(), LimitPriority.MIN),clientLimit);

    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimitMin,configLimit,
        Optional.absent(), LimitPriority.MIN),reportLimitMin);
    Assert.assertEquals(dsQueryBuilder.getLimitForQuery(clientLimit,reportLimit,configLimitMin,
        Optional.absent(), LimitPriority.MIN),configLimitMin);


  }

  @Test
  public void testDateUtilsFunctions(){
    Date date = new Date();
    Date endMonth = DateUtils.ceiling(date, Calendar.MONTH);
    Date startMonth = DateUtils.truncate(date, Calendar.MONTH);
    StringBuilder sb = new StringBuilder();
    sb.append(date)
        .append(date.toString());
    final Timestamp start = new Timestamp(date.getTime());
    sb.append(",").append(start);
    final LocalDate today = LocalDate.of(2023, 6, 22);
//    final LocalDate nextSunday = today.with(next(DayOfWeek.SUNDAY));
//    final LocalDate thisPastSunday = today.with(previous(DayOfWeek.SUNDAY));


  }
}
