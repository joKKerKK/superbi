package com.flipkart.fdp.superbi.cosmos.meta.db;

import com.flipkart.fdp.superbi.cosmos.meta.db.health.HealthStatus;
import com.flipkart.fdp.superbi.cosmos.meta.db.health.SimpleHealthStatus;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.SourceType;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.SessionFactory;

/**
 * Use this class to create transactions.
 * This, along with WorkUnit is basically a helper
 * that initializes, closes / rollbacks the session after a work unit.
 *
 * Typical expectation of use would be to create an Anonymous class
 * subclassing WorkUnit and do work that need to be in a transaction there.
 *
 *
 * Created with IntelliJ IDEA.
 * User: rama
 * Date: 7/1/14
 * Time: 12:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransactionLender {

    private final SessionFactory factory;

    public TransactionLender(SessionFactory factory) {
        this.factory = factory;
    }

    public void execute(WorkUnit unit) {
        unit.doWork(this.factory);
    }

    public HealthStatus getHealth() {
        try {
            final AtomicReference<Boolean> resultSizeCheck = new AtomicReference<Boolean>();
            execute(new WorkUnit() {
                @Override
                public void actualWork() {
                    List result = factory.getCurrentSession().createCriteria(SourceType.class).setMaxResults(1).list();
                    resultSizeCheck.set(result.size() == 1);
                }
            });
            return new SimpleHealthStatus(resultSizeCheck.get(),
                (resultSizeCheck.get() ? "": "No ") + "result fetched from DB");
        } catch (Exception e) {
            return new SimpleHealthStatus(false, "Exception while getting error: " + e.getMessage());
        }
    }
}
