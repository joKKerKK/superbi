package com.flipkart.fdp.superbi.cosmos.cache;

import com.flipkart.fdp.superbi.cosmos.cache.cacheCore.ICacheClient;
import com.flipkart.fdp.superbi.cosmos.cache.metastore.IMetastore;
import com.flipkart.fdp.superbi.cosmos.cache.queryProvider.IQueryProvider;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by piyush.mukati on 14/05/15.
 */
public class CacheCleaner<Query, Value> {
    private static final Logger logger = LoggerFactory.getLogger(CacheCleaner.class);
    private static final String cacheLoggerformatString = "[%s] CACHE class=%s msg=%s"; //class_name  msg


    private ICacheClient<Query, Value> cacheClient;
    private IMetastore<Query> metastore;

    public CacheCleaner(ICacheClient<Query, Value> cacheClient,
                        IMetastore<Query> metastore) {
        this.cacheClient = cacheClient;
        this.metastore = metastore;
    }

    private void cleanForQuery(Query query,IQueryProvider<Query> queryProvider) {
        try{
            cacheClient.remove(query);
            queryProvider.querytoFacts(query).forEach(fact -> {
                metastore.removeFactQuery(fact, query);
        });
    }catch (Exception e){
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            logger.error(String.format(cacheLoggerformatString, new Date(), this.getClass().getName(), "Exception in cleanForQuery, skipping query " + query+"\t"+sw.toString()));
        }
    }

    public void cleanCache(Collection<IQueryProvider<Query>> queryProviders) {
        for(IQueryProvider<Query> queryProvider :queryProviders) {
            queryProvider.getQueries().stream().forEach(query -> cleanForQuery(query,queryProvider));
        }

    }
}


