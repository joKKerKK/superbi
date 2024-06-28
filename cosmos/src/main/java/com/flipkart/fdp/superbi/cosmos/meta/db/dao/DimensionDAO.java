package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
 * Date: 27/02/14
 * Time: 11:45 PM
 */
public class DimensionDAO extends AbstractDAO<Dimension> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public DimensionDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    /**
     *
     * @param dimension
     * @exception RuntimeException if dimension already present or table of dimension or hierarchy not present
     */
    public void save(com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension dimension){
        Dimension internalDimension = getExistingDimension(dimension.getName());
        if(internalDimension == null)
            save(dimension, 1);
        else
            save(dimension, internalDimension.getVersion()+1);
    }

    public void save(com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension dimension, int version){
        Dimension existingDimension = null;
        try {
            existingDimension = getDimensionByName(dimension.getName());
        }catch (RuntimeException e){}
        if(existingDimension !=null)
            throw new RuntimeException("Dimension :" +dimension.getName()+" already exists!!!");
        Dimension internalDimension = new Dimension();
        Table table = getTable(dimension.getTableName());
        Namespace namespace = new NamespaceDAO(getSessionFactory())
                .getNamespaceByOrgAndName(dimension.getOrg(),dimension.getNamespace());
        internalDimension.setTable(table);
        internalDimension.setName(dimension.getName());
        internalDimension.setNamespace(namespace);
        internalDimension.setVisibility(dimension.getVisibility());
        internalDimension.setDescription(dimension.getDescription());
        internalDimension.setType(DataSource.Type.dimension);
        internalDimension.setVersion(version);

        if(dimension.getHierarchies() == null)
            throw new RuntimeException("No hierarchies defined for dimension : "+dimension.getName());
        Set<Hierarchy> internalHierarchies = Sets.newHashSet();
        for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Hierarchy hierarchy : dimension.getHierarchies()){
            com.flipkart.fdp.superbi.cosmos.meta.model.data.Table hierarchyTable = null;
            if(hierarchy.getTableName()!=null){
                hierarchyTable = getTable(hierarchy.getTableName());
            }
            else
                hierarchyTable = table;
            Hierarchy internalHierarchy = new Hierarchy(hierarchy.getName(),hierarchyTable,hierarchy.isHasAll(),internalDimension,hierarchy.getDescription());
            Set<Level> internalLevels = Sets.newLinkedHashSet();
            for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension.Level level : hierarchy.getLevels()){
                Column column = TableDAO.getColumnByName(hierarchyTable, level.getColumn().getName());
                if(column == null)
                    throw new RuntimeException("no column for level : "+level.getName()+" by name : "+level.getColumn().getName());
                Level internalLevel = new Level(level.getName(),level.getOrderByColumn(),column,level.isHasUniqueMember(),level.isHighCardinality(),internalHierarchy, level.getDescription());
                internalLevels.add(internalLevel);
            }
            internalHierarchy.setLevels(Lists.newArrayList(internalLevels));
            internalHierarchies.add(internalHierarchy);
        }
        internalDimension.setHierarchies(internalHierarchies);
        currentSession().save(internalDimension);
    }

    private Table getTable(String tableName){
        return new TableDAO(getSessionFactory()).getTableByName(tableName);
    }

    public Dimension getDimensionByName (String name){
        Dimension dimension =  uniqueResult(criteria().add(eq("name", name)).add(eq("deleted",false)));
        if(dimension == null)
            throw new RuntimeException("no dimension by name :"+name);
        return dimension;
    }

    public Optional<Hierarchy> getHierarchy(String dimensionName, final String hierarchyName){
        Dimension dimension = null;
        try{
            dimension = getDimensionByName(dimensionName);
        }catch (Exception e){
        }
        if(dimension == null)
            return Optional.absent();
        List<Hierarchy> hierarchyList = Lists.newArrayList(Iterables.filter(dimension.getHierarchies(), new Predicate<Hierarchy>() {
            @Override
            public boolean apply(@Nullable Hierarchy input) {
                boolean result = false;
                if (input.getName().equals(hierarchyName))
                    result = true;
                return result;
            }
        }));
        if(hierarchyList.size()!=1){
            return Optional.absent();
        }
        return Optional.of(hierarchyList.get(0));
    }

    public Optional<Level> getLevel(String dimensionName, String hierarchyName, final String levelName){
        Optional<Hierarchy> hierarchyOptional = getHierarchy(dimensionName,hierarchyName);
        if(!hierarchyOptional.isPresent())
            return Optional.absent();
        Hierarchy hierarchy = hierarchyOptional.get();
        List<Level> filteredLevels = Lists.newArrayList(Iterables.filter(hierarchy.getLevels(),new Predicate<Level>() {
            @Override
            public boolean apply(@Nullable Level input) {
                boolean result = false;
                if(input.getName().equals(levelName))
                    result = true;
                return result;
            }
        }));
        if(filteredLevels.size()!=1){
            return Optional.absent();
        }
        return Optional.of(filteredLevels.get(0));
    }

    //can return null if no column is found
    public static Column getColumnByName(Dimension dimension, String columnName){
        Column column = TableDAO.getColumnByName(dimension.getTable(), columnName);
        if(column == null){
            for(Hierarchy hierarchy : dimension.getHierarchies()){
                column = TableDAO.getColumnByName(hierarchy.getTable(),columnName);
                if(column!=null)
                    break;
            }
        }
        return column;
    }

    public Set<Dimension> getDimensions() {
        return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
    }

    public void delete(String dimensionName){
        Dimension dimension = getDimensionByName(dimensionName);
        Set<Hierarchy> hierarchies = dimension.getHierarchies();
        for(Hierarchy hierarchy : hierarchies){
            Set<Level> levels = Sets.newHashSet(hierarchy.getLevels());
            for(Level level: levels){
                level.setDeleted(true);
                currentSession().update(level);
            }
            hierarchy.setDeleted(true);
            currentSession().update(hierarchy);
        }

        final FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(getSessionFactory());
        Set<FactDimensionMapping> factDimensionMappings = factDimensionMappingDAO.getDimensionMappingByDimension(dimension);
        for(FactDimensionMapping factDimensionMapping : factDimensionMappings){
            factDimensionMapping.setDeleted(true);
            currentSession().update(factDimensionMapping);
        }

        dimension.setDeleted(true);
        currentSession().update(dimension);
    }

    public void update(com.flipkart.fdp.superbi.cosmos.meta.model.external.Dimension dimension) {
        Dimension internalDimension = null;
        try{
            internalDimension = getDimensionByName(dimension.getName());
        }catch (Exception e){
        }
        if(internalDimension==null){
            save(dimension);
        }
        else {
            Dimension existingDimension = getDimensionByName(dimension.getName());
            final FactDimensionMappingDAO factDimensionMappingDAO = new FactDimensionMappingDAO(getSessionFactory());
            Set<FactDimensionMapping> factDimensionMappings = factDimensionMappingDAO.getDimensionMappingByDimension(existingDimension);

            int newVersion = internalDimension.getVersion()+1;
            delete(dimension.getName());
            save(dimension,newVersion);

            Dimension updatedDimension = getDimensionByName(dimension.getName());
            for(FactDimensionMapping factDimensionMapping : factDimensionMappings){
                factDimensionMapping.setDimension(updatedDimension);
                factDimensionMapping.setDimensionColumn(getColumnByName(updatedDimension,factDimensionMapping.getDimensionColumn().getName()));
                factDimensionMapping.setDeleted(false);
                currentSession().save(factDimensionMapping);
            }
        }
    }

    private Dimension getExistingDimension(String name){
        Dimension dimension = null;
        List<Dimension> dimensions = getAllVersions(name);
        if (!dimensions.isEmpty()){
            Ordering<Dimension> byVersion = new Ordering<Dimension>() {
                @Override
                public int compare(@Nullable Dimension left, @Nullable Dimension right) {
                    return Ints.compare(left.getVersion(),right.getVersion());
                }
            };
            Collections.sort(dimensions,byVersion);
            dimension = dimensions.get(dimensions.size()-1);
        }
        return dimension;
    }

    private List<Dimension> getAllVersions(String name) {
        return list(criteria().add(eq("name", name)));
    }
}
