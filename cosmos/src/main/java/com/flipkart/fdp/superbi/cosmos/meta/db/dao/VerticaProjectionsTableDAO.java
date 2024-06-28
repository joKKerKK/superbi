package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.VerticaProjectionsTable;
import java.util.List;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Projections;
import org.hibernate.transform.Transformers;


/**
 * Created by chandrasekhar.v on 11/08/15.
 */
public class VerticaProjectionsTableDAO extends AbstractDAO<VerticaProjectionsTable> {

    public VerticaProjectionsTableDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<VerticaProjectionsTable> getProjectionTables(String tableName) {

        Criteria criteria = currentSession().createCriteria(VerticaProjectionsTable.class).setProjection(Projections.projectionList()
                .add(Projections.groupProperty("projectionBaseName"), "projectionBaseName")
                .add(Projections.min("projectionId"), "projectionId")
                .add(Projections.min("segmentationExpression"), "segmentationExpression")
                .add(Projections.min("anchorTableName"), "anchorTableName")
                .add(Projections.min("kSafe"), "kSafe"))
                .add(eq("anchorTableName", tableName))
                .setResultTransformer(Transformers.aliasToBean(VerticaProjectionsTable.class));

        return criteria.list();
    }
}
