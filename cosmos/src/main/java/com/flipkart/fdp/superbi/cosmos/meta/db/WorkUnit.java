package com.flipkart.fdp.superbi.cosmos.meta.db;

import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 *
 * Subclass it and override the #actualWork method to do the required work within a transaction.
 *
 * To change this template use File | Settings | File Templates.
 */
public abstract class WorkUnit {

    final void doWork(SessionFactory sessionFactory) {

        Transaction txn = sessionFactory.getCurrentSession().getTransaction();
        boolean amSessionInitiator = !txn.isActive();
        try {
            if (amSessionInitiator)
                txn.begin();
            actualWork();
            if (amSessionInitiator)
                sessionFactory.getCurrentSession().getTransaction().commit();
        } catch (RuntimeException e) {
            sessionFactory.getCurrentSession().getTransaction().rollback();
            throw e;
        } finally {
            if (amSessionInitiator)
                sessionFactory.getCurrentSession().close();
        }
    }

    public abstract void actualWork();

    /**
     * Optionally override this method to give the required message
     * that'll get logged in case of error.
     *
     * @return
     */
    public String getMessage() {
        return "";
    }
}
