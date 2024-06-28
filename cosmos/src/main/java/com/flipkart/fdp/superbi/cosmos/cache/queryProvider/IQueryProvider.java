package com.flipkart.fdp.superbi.cosmos.cache.queryProvider;

import java.util.Collection;

/**
 * Created by piyush.mukati on 13/05/15.
 */
public interface IQueryProvider<T> {


    Collection<T> getQueries();
    Collection<String> querytoFacts(T query) ;


    }
