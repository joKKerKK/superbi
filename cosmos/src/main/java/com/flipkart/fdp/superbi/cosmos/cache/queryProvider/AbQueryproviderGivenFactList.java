package com.flipkart.fdp.superbi.cosmos.cache.queryProvider;

import com.flipkart.fdp.superbi.cosmos.cache.metastore.IMetastore;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by piyush.mukati on 21/05/15.
 */
public abstract class AbQueryproviderGivenFactList<Query> implements IQueryProvider<Query> {

    private IMetastore<Query> metastore;

    public AbQueryproviderGivenFactList(IMetastore<Query> metastore) {
        this.metastore = metastore;
    }

    abstract public Collection<String> getFacts();


    @Override
    public Collection<Query> getQueries() {
        Set<Query> set = new HashSet<Query>();
        Collection<String> facts = getFacts();
        if (facts == null)
            return set;

        for (String fact : facts) {
            set.addAll(metastore.getQuerysForFact(fact));
        }
        return set;
    }
}
