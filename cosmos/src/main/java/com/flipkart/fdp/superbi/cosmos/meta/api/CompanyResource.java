package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.db.TransactionLender;
import com.flipkart.fdp.superbi.cosmos.meta.db.WorkUnit;
import com.flipkart.fdp.superbi.cosmos.meta.db.dao.CompanyDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Company;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebCompany;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.SessionFactory;

/**
 * User: aartika
 * Date: 5/9/14
 */
public class CompanyResource {

    private CompanyDAO companyDAO;
    private TransactionLender transactionLender;

    public CompanyResource(SessionFactory sessionFactory) {
        this.companyDAO = new CompanyDAO(sessionFactory);
        this.transactionLender = new TransactionLender(sessionFactory);
    }

    public List<WebCompany> listCompanies() {
        final AtomicReference<ArrayList<WebCompany>> webCompanies = new AtomicReference<ArrayList<WebCompany>>();
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                webCompanies.set(new ArrayList<WebCompany>(Collections2.transform(companyDAO.getAll(), new Function<Company, WebCompany>() {
                    @Override
                    public WebCompany apply(Company input) {
                        return new WebCompany(input.getFullName(), input.getName());
                    }
                })));
            }
        });
        return webCompanies.get();
    }
}
