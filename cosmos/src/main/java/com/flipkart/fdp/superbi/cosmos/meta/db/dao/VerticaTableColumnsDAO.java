package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.VerticaTableColumn;
import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.transform.Transformers;

/**
 * Created by chandrasekhar.v on 24/08/15.
 */
public class VerticaTableColumnsDAO extends AbstractDAO<VerticaTableColumn> {

    public VerticaTableColumnsDAO(SessionFactory sessionFactory) { super(sessionFactory); }

    public List<VerticaTableColumn> getVerticaTableColumns(String tableName) {
        List<VerticaTableColumn> verticaTableColumns = currentSession().createCriteria(VerticaTableColumn.class).setProjection(Projections.projectionList()
                .add(Projections.property("columnId"), "columnId").add(Projections.property("columnName"), "columnName")
                .add(Projections.property("tableName"), "tableName").add(Projections.property("tableSchema"), "tableSchema")
                .add(Projections.property("dataType"), "dataType").add(Projections.property("columnPosition"), "columnPosition"))
                .add(Restrictions.eq("tableName", tableName))
                .addOrder(Order.asc("columnPosition")).setResultTransformer(Transformers.aliasToBean(VerticaTableColumn.class)).list();

        return verticaTableColumns;
    }
}
