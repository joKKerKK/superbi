package com.flipkart.fdp.superbi.cosmos.cache;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.cache.dataSource.IDataSource;
import com.flipkart.fdp.superbi.cosmos.cache.metastore.IMetastore;
import com.flipkart.fdp.superbi.cosmos.cache.queryProvider.IQueryProvider;
import com.flipkart.fdp.superbi.cosmos.cache.util.Util;
import com.flipkart.fdp.superbi.dsl.utils.Timer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by piyush.mukati on 13/05/15.
 */
public class CacheFiller<Query, Value> {
    private static final Logger logger = LoggerFactory.getLogger(CacheCleaner.class);
    private static final String cacheLoggerformatString = "[%s] CACHE class=%s msg=%s"; //class_name  msg

    private ICacheClient<Query, Value> cacheClient;
    private IMetastore<Query> metastore;
    private IDataSource<Query, Value> dataSource;

    public CacheFiller(ICacheClient cacheClient, IDataSource dataSource,
                       IMetastore metastore) {
        this.cacheClient = cacheClient;
        this.dataSource = dataSource;
        this.metastore = metastore;

    }

    private void fillForQuery(Query query,IQueryProvider<Query> queryProvider) {
       try{
        final Timer executionTimer = new Timer().start();
        Optional<Value> resOp = dataSource.execute(query);
         executionTimer.stop();


        if(resOp.isPresent()){
    Value res=resOp.get();
            if (Util.isCacheCandidate(executionTimer.getTimeTakenMs(), -1)) {
            queryProvider.querytoFacts(query).forEach(f -> {
                metastore.addQueryForFact(f, query);
            });
            cacheClient.set(query, 24*3600, res);
        }
        }
       }catch (Exception e) {
           StringWriter sw = new StringWriter();
           e.printStackTrace(new PrintWriter(sw));
           logger.error(String.format(cacheLoggerformatString, new Date(), this.getClass().getName(), "Exception in fillForQuery, skipping query " + query +"\t"+sw.toString()));


       }
    }

    public void fillCache(Collection<IQueryProvider<Query>> queryProviders) {
for(IQueryProvider<Query> queryProvider :queryProviders) {
    queryProvider.getQueries().stream().forEach(query -> fillForQuery(query,queryProvider ));
}
    }


}
