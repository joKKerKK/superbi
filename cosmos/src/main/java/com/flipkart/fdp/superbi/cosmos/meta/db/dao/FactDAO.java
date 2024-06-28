package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.hibernate.SessionFactory;

/**
 * User: aniruddha.gangopadhyay
 * Date: 28/02/14
 * Time: 12:17 AM
 */
public class FactDAO extends AbstractDAO<Fact> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public FactDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    /**
     *
     * @param fact
     * @exception RuntimeException if fact already exists / table not present / no measure
     *            are defined / column associations for measure or degenerate dimension fail
     */
    public void save(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact fact){
        Fact internalFact = getExistingFact(fact.getName());
        if(internalFact == null)
            save(fact,1);
        else
            save(fact,internalFact.getVersion()+1);
    }

    public void save(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact fact, int version){
        Fact existingFact = null;
        try {
            existingFact = getFactByName(fact.getName());
        }catch (RuntimeException e){}
        if(existingFact != null)
            throw new RuntimeException("Fact :"+ fact.getName()+" already exists");
        Fact internalFact = new Fact();
        Table table = getTable(fact.getTableName());
        Namespace namespace = new NamespaceDAO(getSessionFactory())
                .getNamespaceByOrgAndName(fact.getOrg(),fact.getNamespace());
        internalFact.setTable(table);
        internalFact.setName(fact.getName());
        internalFact.setDescription(fact.getDescription());
        internalFact.setVisibility(fact.getVisibility());
        internalFact.setNamespace(namespace);
        internalFact.setType(DataSource.Type.fact);
        internalFact.setVersion(version);

        if(fact.getMeasures()==null)
            throw new RuntimeException("no measures defined for fact : " +fact.getName());
        Set<Measure> internalMeasures = Sets.newHashSet();
        for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.Measure measure : fact.getMeasures()){
            Column column = null;
            Measure internalMeasure = null;
            if(measure.getColumnName()!=null) {
                column = TableDAO.getColumnByName(table,measure.getColumnName());
                if(column == null)
                    throw new RuntimeException("no column for measure :"+measure.getName()+"by name : "+measure.getColumnName());
                internalMeasure = new Measure(measure.getName(),measure.getType(),internalFact,null,column,measure.getAggregator(),measure.getFormat(),measure.getDescription());
            }
            else if(measure.getDefinition()!=null)
                internalMeasure = new Measure(measure.getName(),measure.getType(),internalFact,measure.getDefinition(),column,measure.getAggregator(),measure.getFormat(),measure.getDescription());
            else
                throw new RuntimeException("no association found for measure :"+ measure.getName());
            internalMeasures.add(internalMeasure);
        }

        Set<DegenerateDimension> internalDegenerateDimensions = Sets.newHashSet();
        if(fact.getDegenerateDimensions()!=null){
            for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DegenerateDimension degenerateDimension : fact.getDegenerateDimensions()){
                if(degenerateDimension.getColumnName()==null)
                    throw new RuntimeException("no association found for degenerate dimension : "+degenerateDimension.getName());
                Column column  = TableDAO.getColumnByName(table,degenerateDimension.getColumnName());
                if(column == null)
                    throw new RuntimeException("no column for measure :"+degenerateDimension.getName()+"by name : "+degenerateDimension.getColumnName());
                DegenerateDimension internalDegenerateDimension = new DegenerateDimension(degenerateDimension.getName(),internalFact,column, degenerateDimension.getDescription());
                internalDegenerateDimensions.add(internalDegenerateDimension);
            }
        }
        internalFact.setMeasures(internalMeasures);
        internalFact.setDegenerateDimensions(internalDegenerateDimensions);
        currentSession().save(internalFact);

        for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact.DimensionMapping dimensionMapping : fact.getDimensionsMapping()) {
            FactDimensionMapping factDimensionMapping = new FactDimensionMapping();
            factDimensionMapping.setFact(internalFact);
            com.flipkart.fdp.superbi.cosmos.meta.model.data.Dimension dimension = new DimensionDAO(getSessionFactory()).getDimensionByName(dimensionMapping.getDimensionName());
            factDimensionMapping.setDimension(dimension);
            factDimensionMapping.setDimensionType(dimensionMapping.getDimensionType());
            Column factColumn = TableDAO.getColumnByName(table,dimensionMapping.getFactColumnName());
            if(factColumn == null)
                throw new RuntimeException("no column found by name : "+ dimensionMapping.getFactColumnName());
            factDimensionMapping.setFactColumn(factColumn);
            Column dimensionColumn = DimensionDAO.getColumnByName(dimension,dimensionMapping.getDimensionColumnName());
            if(dimensionColumn == null)
                throw new RuntimeException("no column found by name : "+ dimensionMapping.getDimensionColumnName());
            factDimensionMapping.setDimensionColumn(dimensionColumn);
            currentSession().save(factDimensionMapping);
        }
    }

    private Table getTable(String tableName){
        return new TableDAO(getSessionFactory()).getTableByName(tableName);
    }

    public Fact getFactByName(String name){
        Fact fact =  uniqueResult(criteria().add(eq("name", name)).add(eq("deleted",false)));
        if(fact==null)
            throw new RuntimeException("no fact by name :"+ name);
        return fact;
    }

    public Set<Fact> getFacts(){
        return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
    }

    public void delete(Fact fact) {
        Set<Measure> measures = fact.getMeasures();
        for(Measure measure : measures){
            measure.setDeleted(true);
            currentSession().update(measure);
        }
        Set<DegenerateDimension> degenerateDimensions = fact.getDegenerateDimensions();
        for(DegenerateDimension degenerateDimension : degenerateDimensions){
            degenerateDimension.setDeleted(true);
            currentSession().update(degenerateDimension);
        }
        FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(getSessionFactory());
        Set<FactDimensionMapping> factDimensionMappings = factDimensionMappingDAO.getDimensionMappingByFact(fact);
        for(FactDimensionMapping factDimensionMapping : factDimensionMappings){
            factDimensionMapping.setDeleted(true);
            currentSession().update(factDimensionMapping);
        }
        fact.setDeleted(true);
        currentSession().update(fact);
    }

    public void delete(String factName){
        delete(getFactByName(factName));
    }

    public void update(com.flipkart.fdp.superbi.cosmos.meta.model.external.Fact fact) {
        Fact internalFact = null;
        try{
            internalFact = getFactByName(fact.getName());
        }catch (Exception e){
        }
        if(internalFact==null){
            save(fact);
        }
        else {
            int newVersion = internalFact.getVersion()+1;
            delete(fact.getName());
            save(fact,newVersion);
        }
    }

    private Fact getExistingFact(String name){
        Fact fact = null;
        List<Fact> facts = getAllVersions(name);
        if (!facts.isEmpty()){
            Ordering<Fact> byVersion = new Ordering<Fact>() {
                @Override
                public int compare(@Nullable Fact left, @Nullable Fact right) {
                    return Ints.compare(left.getVersion(),right.getVersion());
                }
            };
            Collections.sort(facts,byVersion);
            fact = facts.get(facts.size()-1);
        }
        return fact;
    }

    private List<Fact> getAllVersions(String name) {
        return list(criteria().add(eq("name", name)));
    }
}
