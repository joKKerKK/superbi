package com.flipkart.fdp.superbi.refresher.dao.result;

import javax.validation.constraints.NotNull;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.BaseStream;


public  abstract class QueryResult implements BaseStream<List<Object>,QueryResult>{
    @Override
    public abstract Iterator<List<Object>> iterator();

    @Override
    public Spliterator<List<Object>> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isParallel() {
        return false;
    }

    @Override
    public QueryResult sequential() {
        return this;
    }

    @Override
    public QueryResult parallel() {
        return this;
    }

    @Override
    public QueryResult unordered() {
        return this;
    }

    @Override
    public QueryResult onClose(Runnable closeHandler) {
        // Ignore
        return this;
    }

    @NotNull
    public abstract List<String> getColumns();
}
