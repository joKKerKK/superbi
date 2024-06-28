package com.flipkart.fdp.superbi.cosmos.cache.metastore;

import java.util.Collection;

/**
 * Created by piyush.mukati on 14/05/15.
 */
public interface IMetastore<Query> {

    void addQueryForFact(String fact, Query query);

    Collection<Query> getQuerysForFact(String fact);

    void removeFact(String fact);

    void removeFactQuery(String fact, Query query);


}
