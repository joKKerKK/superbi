package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Dimension;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Fact;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.FactDimensionMapping;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.hibernate.SessionFactory;

/**
 * User: aniruddha.gangopadhyay
 * Date: 28/02/14
 * Time: 12:39 AM
 */
public class FactDimensionMappingDAO extends AbstractDAO<FactDimensionMapping> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public FactDimensionMappingDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Set<Fact> getFactsByDimension(String dimensionName){
        Dimension dimension = new DimensionDAO(getSessionFactory()).getDimensionByName(dimensionName);
        if(dimension==null)
            throw new RuntimeException("No dimension by name : "+dimensionName);
        List<FactDimensionMapping> factDimensionMappings = list(criteria().add(eq("dimension",dimension)).add(eq("deleted",false)));
        return Sets.newHashSet(Iterables.transform(factDimensionMappings,new Function<FactDimensionMapping, Fact>() {
            @Override
            public Fact apply(FactDimensionMapping factDimensionMapping) {
                return factDimensionMapping.getFact();
            }
        }));
    }

    public Set<Dimension> getDimensionsByFact(String factName){
        Fact fact = new FactDAO(getSessionFactory()).getFactByName(factName);
        if(fact==null)
            throw new RuntimeException("No fact by name : "+factName);
        List<FactDimensionMapping> factDimensionMappings = list(criteria().add(eq("fact",fact)).add(eq("deleted",false)));
        return Sets.newHashSet(Iterables.transform(factDimensionMappings,new Function<FactDimensionMapping, Dimension>() {
            @Override
            public Dimension apply(FactDimensionMapping factDimensionMapping) {
                return factDimensionMapping.getDimension();
            }
        }));
    }

    public Set<FactDimensionMapping> getDimensionMappingByFact(Fact fact){
        return Sets.newHashSet(list(criteria().add(eq("fact",fact)).add(eq("deleted",false))));
    }

    public Set<FactDimensionMapping> getDimensionMappingByDimension(Dimension dimension){
        return Sets.newHashSet(list(criteria().add(eq("dimension",dimension)).add(eq("deleted",false))));
    }
}
