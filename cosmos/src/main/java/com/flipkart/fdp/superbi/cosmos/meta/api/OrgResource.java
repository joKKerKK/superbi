package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.OrgDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Org;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebOrg;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.SessionFactory;

/**
 * User: aartika
 * Date: 3/23/14
 */
public class  OrgResource {
    
    private final OrgDAO orgDAO;
    private final TransactionLender transactionLender;
    

    public OrgResource(SessionFactory sessionFactory) {
        orgDAO = new OrgDAO(sessionFactory);
        transactionLender = new TransactionLender(sessionFactory);
    }

    public List<WebOrg> listAllOrgs() {
        final AtomicReference<List<WebOrg>> webOrgs = new AtomicReference<List<WebOrg>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                List<Org> org = new ArrayList<Org>(orgDAO.getOrgs());
                webOrgs.set(new ArrayList<WebOrg>(Collections2.transform(org, new Function<Org, WebOrg>() {
                    @Override
                    public WebOrg apply(Org input) {
                        return new WebOrg(input);
                    }
                })));
            }
        });
        return webOrgs.get();
    }
}
