package com.flipkart.fdp.superbi.refresher.dao;


import com.flipkart.fdp.superbi.exceptions.ClientSideException;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamClient;
import com.flipkart.fdp.superbi.refresher.dao.fstream.FStreamDataSourceDao;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FStreamQuery;
import com.flipkart.fdp.superbi.refresher.dao.fstream.requests.FstreamRequest;
import com.flipkart.fdp.superbi.refresher.dao.query.DataSourceQuery;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.isA;

@RunWith(PowerMockRunner.class)
public class FStreamDataSourceDaoTest {

    @Mock
    private FStreamClient FStreamClient;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static final String SAMPLE_RESULT = "[ { \"dimensions\": { \"cf.mpid\": \"FLIPKART\" }, \"aggregateData\": { \"cf.visit_ids\": 13982297, \"cf.dev_ids\": 12041320 } }, { \"dimensions\": { \"cf.mpid\": \"GROCERY\" }, \"aggregateData\": { \"cf.visit_ids\": 177461, \"cf.dev_ids\": 166712 } } ]";
    private static final List<List<Object>> rowList = Arrays.asList(Arrays.asList("FLIPKART",13982297,12041320),Arrays.asList("GROCERY",177461,166712));
    private static final JSONArray response = new JSONArray(SAMPLE_RESULT);

    @Test
    public void testFStreamQuerySuccess(){
        FStreamDataSourceDao fstreamDataSourceDao = new FStreamDataSourceDao(FStreamClient);
        FStreamQuery fStreamQuery = generateFStreamQuery();
        Mockito.when(FStreamClient.getAggregatedData(fStreamQuery.getFstreamRequest(),fStreamQuery.getFstreamId())).thenReturn(response);
        QueryResult queryResult = fstreamDataSourceDao.getStream(DataSourceQuery.builder().nativeQuery(fStreamQuery).build());

        Assert.assertEquals(queryResult.iterator().hasNext(),true);
        Assert.assertEquals(queryResult.iterator().next(),rowList.get(0));
        Assert.assertEquals(queryResult.iterator().hasNext(),true);
        Assert.assertEquals(queryResult.iterator().next(),rowList.get(1));
        Assert.assertEquals(queryResult.iterator().hasNext(),false);

        Assert.assertEquals(queryResult.getColumns(),fStreamQuery.getOrderedFstreamColumns());
//        Assert.assertEquals(queryResult.iterator().next(),);
    }

    @Test
    public void testFStreamQueryFailureFromClient(){
        expectedException.expect(ClientSideException.class);
        expectedException.expectCause(isA(JSONException.class));
        Mockito.when(FStreamClient.getAggregatedData(Mockito.any(),Mockito.any())).thenThrow(new ClientSideException(new JSONException("Incorrect Json Format")));

        FStreamQuery fStreamQuery = generateFStreamQuery();
        FStreamDataSourceDao fstreamDataSourceDao = new FStreamDataSourceDao(FStreamClient);

        fstreamDataSourceDao.getStream(DataSourceQuery.builder().nativeQuery(fStreamQuery).build());
    }

    private FStreamQuery generateFStreamQuery() {
        List<String> orderedColumnList = Arrays.asList("mpid","visit_ids","dev_ids");
        return new FStreamQuery(new FstreamRequest(null,null,null,null,null,null),"34",orderedColumnList);
    }
}
