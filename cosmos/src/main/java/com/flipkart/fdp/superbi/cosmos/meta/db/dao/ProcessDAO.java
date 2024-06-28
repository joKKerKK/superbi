package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Process;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

/**
 * Created by amruth.s on 29/09/14.
 */

public class ProcessDAO extends AbstractDAO<com.flipkart.fdp.superbi.cosmos.meta.model.data.Process> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public ProcessDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(final Process process) {
        currentSession().save(process);
    }

    public void delete(final Process process) {
        currentSession().delete(process);
    }

    public void deleteAllBy(final String hostName) {
        Criteria criteria = currentSession().createCriteria(Process.class);
        criteria.add(Restrictions.eq("user", hostName)) ;
        for(Object process :criteria.list()) {
            currentSession().delete(process);
        }
    }
}
