package com.flipkart.fdp.superbi.refresher.dao.druid;


import com.flipkart.fdp.superbi.exceptions.ServerSideException;
import com.flipkart.fdp.superbi.refresher.dao.druid.requests.DruidQuery;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class DruidDataSourceDaoTest {

  @Mock
  private DruidClient druidClient;

  private static final String SAMPLE_RESULT = "[\n"
      + "    {\n"
      + "        \"visitors\": 13982297,\n"
      + "        \"platform\": \"AndroidApp\"\n"
      + "    },\n"
      + "    {\n"
      + "        \"visitors\": 166712,\n"
      + "        \"platform\": \"Website\"\n"
      + "    },    \n"
      + "]";
  private static final List<List<Object>> rowList = Arrays
      .asList(Arrays.asList("AndroidApp",13982297),Arrays.asList("Website",166712));
  private static final JSONArray response = new JSONArray(SAMPLE_RESULT);

  @Test
  public void testDruidQuerySuccess(){

    DruidDataSourceDao druidDataSourceDao = new DruidDataSourceDao(druidClient);
    DruidQuery druidQuery = generateDruidQuery();
    Mockito.when(druidClient.getDataFromQuery(druidQuery)).thenReturn(response);
    QueryResult queryResult = druidDataSourceDao.getStream(
        DataSourceQuery.builder().nativeQuery(druidQuery).build());

    Assert.assertEquals(queryResult.iterator().hasNext(),true);
    Assert.assertEquals(queryResult.iterator().next(),rowList.get(0));
    Assert.assertEquals(queryResult.iterator().hasNext(),true);
    Assert.assertEquals(queryResult.iterator().next(),rowList.get(1));
    Assert.assertEquals(queryResult.iterator().hasNext(),false);

    Assert.assertEquals(queryResult.getColumns(),druidQuery.getHeaderList());
  }

  @Test(expected = ServerSideException.class)
  public void testDruidFailureFromClient(){
    Mockito.when(druidClient.getDataFromQuery(Mockito.any())).thenThrow(new ServerSideException("Incorrect Json Format"));

    DruidDataSourceDao druidDataSourceDao = new DruidDataSourceDao(druidClient);
    DruidQuery druidQuery = generateDruidQuery();

    druidDataSourceDao.getStream(DataSourceQuery.builder().nativeQuery(druidQuery).build());
  }

  private DruidQuery generateDruidQuery() {
    List<String> headerList = Arrays.asList("platform","visitors");
    return new DruidQuery("select \"platform\" as platform, \"visitors\" as visitors from euclid_ooo_stream where TIMESTAMP_TO_MILLIS(__time)>=1577817000000 and TIMESTAMP_TO_MILLIS(__time)<1609439400000 limit 10000",headerList,new HashMap<>());
  }
}
