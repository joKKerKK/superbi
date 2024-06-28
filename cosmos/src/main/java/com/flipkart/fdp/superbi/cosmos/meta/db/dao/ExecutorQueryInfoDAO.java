package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.ExecutorQueryInfoLog;
import org.hibernate.SessionFactory;

/**
 * Created by arun.khetarpal on 03/08/15.
 */
public class ExecutorQueryInfoDAO extends AbstractDAO<ExecutorQueryInfoLog> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public ExecutorQueryInfoDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(final ExecutorQueryInfoLog log) {
        currentSession().save(log);
    }

    public ExecutorQueryInfoLog getById(long id) {
        return get(id);
    }
}
