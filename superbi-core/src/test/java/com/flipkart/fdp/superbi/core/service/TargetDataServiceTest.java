package com.flipkart.fdp.superbi.core.service;

import com.beust.jcommander.internal.Lists;
import com.flipkart.fdp.superbi.core.model.TargetDataResponse;
import com.flipkart.fdp.superbi.core.model.TargetDataSeries;
import com.flipkart.fdp.superbi.dsl.query.SelectColumn;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by akshaya.sharma on 22/04/20
 */
@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class TargetDataServiceTest {
  @Test
  public void testCumulativeSeries() {
    TargetDataResponse originalTargetDataResponse = getSampleTargetDataResponseCumulative();
    TargetDataResponse targetDataResponse = getSampleTargetDataResponse();
    TargetDataService.computeTargetData(targetDataResponse, SelectColumn.SeriesType.CUMULATIVE);

    Assert.assertTrue(
        EqualsBuilder.reflectionEquals(originalTargetDataResponse.getData(),targetDataResponse.getData()));
  }

  @Test
  public void testInstantaneousSeries() {
    TargetDataResponse originalTargetDataResponse = getSampleTargetDataResponse();
    TargetDataResponse targetDataResponse = getSampleTargetDataResponse();
    TargetDataService.computeTargetData(targetDataResponse, SelectColumn.SeriesType.INSTANTANEOUS);

    Assert.assertTrue(
        EqualsBuilder.reflectionEquals(originalTargetDataResponse.getData(),targetDataResponse.getData()));
  }

  private TargetDataResponse getSampleTargetDataResponseCumulative() {
    TargetDataSeries dataSeries1 = new TargetDataSeries("target", "metric");
    // 1, 5, 4
    List<Object[]> data1 = Lists.newArrayList(
        Lists.newArrayList( "2019-10-12T00:00:00.000+05:30", 1).toArray(),
        Lists.newArrayList( "2019-10-12T01:00:00.000+05:30", 6).toArray(),
        Lists.newArrayList( "2019-10-12T02:00:00.000+05:30", 10).toArray()
    );

    TargetDataSeries dataSeries2 = new TargetDataSeries("target", "metric");
    // 10, 5, 3
    List<Object[]> data2 = Lists.newArrayList(
        Lists.newArrayList( "2019-10-12T00:00:00.000+05:30", 10).toArray(),
        Lists.newArrayList( "2019-10-12T01:00:00.000+05:30", 15).toArray(),
        Lists.newArrayList( "2019-10-12T02:00:00.000+05:30", 18).toArray()
    );

    Map<String, List<Object[]>> data = new HashMap<>();
    data.put(dataSeries1.getKey(), data1);
    data.put(dataSeries2.getKey(), data2);

    return TargetDataResponse.builder()
        .data(data)
        .dataSeries(Lists.newArrayList(dataSeries1, dataSeries2))
        .targetMappings(Lists.newArrayList())
        .queryResponses(Lists.newArrayList())
        .build();
  }

  private TargetDataResponse getSampleTargetDataResponse() {
    TargetDataSeries dataSeries1 = new TargetDataSeries("target", "metric");
    List<Object[]> data1 = Lists.newArrayList(
        Lists.newArrayList( "2019-10-12T00:00:00.000+05:30", 1).toArray(),
        Lists.newArrayList( "2019-10-12T01:00:00.000+05:30", 5).toArray(),
        Lists.newArrayList( "2019-10-12T02:00:00.000+05:30", 4).toArray()
    );

    TargetDataSeries dataSeries2 = new TargetDataSeries("target", "metric");
    List<Object[]> data2 = Lists.newArrayList(
        Lists.newArrayList( "2019-10-12T00:00:00.000+05:30", 10).toArray(),
        Lists.newArrayList( "2019-10-12T01:00:00.000+05:30", 5).toArray(),
        Lists.newArrayList( "2019-10-12T02:00:00.000+05:30", 3).toArray()
    );

    Map<String, List<Object[]>> data = new HashMap<>();
    data.put(dataSeries1.getKey(), data1);
    data.put(dataSeries2.getKey(), data2);

    return TargetDataResponse.builder()
        .data(data)
        .dataSeries(Lists.newArrayList(dataSeries1, dataSeries2))
        .targetMappings(Lists.newArrayList())
        .queryResponses(Lists.newArrayList())
        .build();
  }

  @SneakyThrows
  private Date getDate(String isoString) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("IST"));
    return sdf.parse(isoString);
  }
}
