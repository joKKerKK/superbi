package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.in;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.VerticaProjectionColumns;
import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.transform.Transformers;

/**
 * Created by chandrasekhar.v on 11/08/15.
 */
public class VerticaProjectionColumnsDAO extends AbstractDAO<VerticaProjectionColumns> {

    public VerticaProjectionColumnsDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<VerticaProjectionColumns> getProjectionColumns (List<Long> projectionIds) {
        List<VerticaProjectionColumns> verticaProjectionColumns = currentSession().createCriteria(VerticaProjectionColumns.class).setProjection(Projections.projectionList()
                .add(Projections.property("columnId"), "columnId").add(Projections.property("tableName"), "tableName")
                .add(Projections.property("tableColumnName"), "tableColumnName").add(Projections.property("projectionName"),"projectionName")
                .add(Projections.property("projectionId"), "projectionId").add(Projections.property("projectionColumnName"), "projectionColumnName")
                .add(Projections.property("dataType"), "dataType").add(Projections.property("encodingType"), "encodingType")
                .add(Projections.property("sortPosition"), "sortPosition").add(Projections.property("columnPosition"), "columnPosition"))
                .add(in("projectionId",projectionIds)).addOrder(Order.asc("columnPosition"))
                .setResultTransformer(Transformers.aliasToBean(VerticaProjectionColumns.class)).list();

        return verticaProjectionColumns;
    }
}
