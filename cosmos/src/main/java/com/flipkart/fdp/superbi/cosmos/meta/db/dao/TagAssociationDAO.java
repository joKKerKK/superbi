package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.TagAssociation;
import org.hibernate.SessionFactory;

/**
 * Created by ankur.mishra on 17/02/15.
 */
public class TagAssociationDAO extends AbstractDAO<TagAssociation> {

    public TagAssociationDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public TagAssociation getDataSource(String dataSourceName){
        return uniqueResult(criteria().add(eq("dataSourceName", dataSourceName)));
    }

}
