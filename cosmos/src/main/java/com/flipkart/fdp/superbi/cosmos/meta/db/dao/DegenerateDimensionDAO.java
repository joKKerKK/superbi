package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.DegenerateDimension;
import java.util.List;
import org.hibernate.SessionFactory;

/**
 * Created by amruth.s on 20/12/14.
 */
public class DegenerateDimensionDAO extends AbstractDAO<DegenerateDimension> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public DegenerateDimensionDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<DegenerateDimension> getDegeneratesBelongingToTheSameHierarchyAs(String factName, String degDimName) {
        String hql  = "FROM com.flipkart.fdp.superbi.cosmos.meta.model.data.DegenerateDimension AS dd where dd.fact.name='"+ factName +"' and (dd.name='"+degDimName+"' or dd.level.hierarchy.id in (select dds.level.hierarchy.id FROM com.flipkart.fdp.superbi.cosmos.meta.model.data.DegenerateDimension AS dds where dds.name='"+degDimName+"')) order by dd.level.id";
        return  currentSession().createQuery(hql).list();
    }
}
