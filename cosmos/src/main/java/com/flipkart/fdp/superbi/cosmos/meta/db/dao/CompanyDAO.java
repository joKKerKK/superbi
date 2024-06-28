package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.model.data.Company;
import org.hibernate.SessionFactory;

/**
 * User: aartika
 * Date: 5/9/14
 */
public class CompanyDAO extends BaseDAO<Company> {

    public CompanyDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
}
