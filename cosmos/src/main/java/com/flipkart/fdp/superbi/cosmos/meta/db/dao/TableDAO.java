package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.*;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.View;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;

/**
 * User: aniruddha.gangopadhyay
 * Date: 27/02/14
 * Time: 11:27 PM
 */
public class TableDAO extends AbstractDAO<Table> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public TableDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    /**
     *
     * @param table
     *
     * @exception RuntimeException if table already exists or source for the table is not present
     */
    public void save (com.flipkart.fdp.superbi.cosmos.meta.model.external.Table table){
        Table internalTable = getExistingTable(table.getName());
        if(internalTable == null)
            save(table, 1);
        else
            save(table,internalTable.getVersion()+1);
    }

    public void save(com.flipkart.fdp.superbi.cosmos.meta.model.external.Table table, int version){
        Table existingTable = null;
        try{
            existingTable = getTableByName(table.getName());
        }catch (RuntimeException e){}
        if(existingTable != null)
            throw new RuntimeException("Table :"+table.getName()+" already exists!!!");
        Table internalTable = new Table();
        Source source = getSource(table.getSourceName());
        Namespace namespace = new NamespaceDAO(getSessionFactory())
                .getNamespaceByOrgAndName(table.getOrg(),table.getNamespace());;
        internalTable.setName(table.getName());
        internalTable.setNamespace(namespace);
        internalTable.setVisibility(table.getVisibility());
        internalTable.setLastRefresh(table.getLastRefresh());
        internalTable.setLastUpdate(new Date());
        internalTable.setOwner(table.getOwner());
        internalTable.setManager(table.getManager());
        internalTable.setProcessPipeline(table.getProcessPipeline().name());
        internalTable.setPublished(table.isPublished());
        internalTable.setStatus(table.getStatus());
        internalTable.setDescription(table.getDescription());
        internalTable.setIncrMode(table.getIncrMode());
        internalTable.setSource(source);
        internalTable.setType(DataSource.Type.raw);
        internalTable.setVersion(version);
        internalTable.setProcessId(table.getProcessId());
        currentSession().save(internalTable);
        for(com.flipkart.fdp.superbi.cosmos.meta.model.external.Table.Column column : table.getColumns()){
            Column internalColumn = new Column(internalTable,column.getName(),column.getType().name(), column.getMaxLength(), column.getDescription(),column.isPrimary(), column.isPartitioned());
            currentSession().save(internalColumn);
        }
    }

    private Namespace getNamespace(View.Namespace namespace){
        return new NamespaceDAO(getSessionFactory()).getNamespaceByOrgAndName(namespace.getOrg(),namespace.getNamespace());
    }

    private Source getSource(String sourceName){
        return new SourceDAO(getSessionFactory()).getSourceByName(sourceName);
    }

    public Table getTableByName(String name){
        Table table = uniqueResult(criteria().add(eq("name",name)).add(eq("deleted",false)));
        if(table == null)
            throw new RuntimeException("no table by name :"+ name);
        return table;
    }

    public Table getTableById(int id){
        Table table = uniqueResult(criteria().add(eq("id",id)).add(eq("deleted",false)));
        if(table == null)
            throw new RuntimeException("no table with id :"+ id);
        return table;
    }

    public Set<Table> getTables(Optional<View.Entity.ProcessPipeline> pipeline){
        return getTables(pipeline, Optional.<String>absent(), Optional.<Boolean>of(true), Optional.<Source>absent());
    }

    public Set<Table> getTables(Optional<View.Entity.ProcessPipeline> pipeline, Optional<String> owner, Optional<Boolean> isPublished, Optional<Source> source){
        Criteria criteria = criteria();
        if(pipeline.isPresent())
            criteria.add(eq("processPipeline",pipeline.get().name()));
        if(owner.isPresent())
            criteria.add(eq("owner",owner.get()));
        if(isPublished.isPresent())
            criteria.add(eq("published",isPublished.get()));
        if(source.isPresent())
            criteria.add(eq("source",source));
        return Sets.newHashSet(list(criteria.add(eq("deleted", false))));
    }

    //can return null if no column is found
    public static Column getColumnByName(Table table, final String columnName){
        final List<Column> filtered = FluentIterable
            .from(table.getColumns())
            .filter(new Predicate<Column>() {
                @Override
                public boolean apply(final Column input) {
                    boolean result = false;
                    if(input.getName().equals(columnName))
                        result = true;
                    return result;
                }
            })
            .toList();
        if(filtered.isEmpty())
            return null;
        else
            return filtered.get(0);
    }

    public void delete(String tableName){
        Table table = getTableByName(tableName);
        Set<Column> columns = table.getColumns();
        for(Column column : columns){
            column.setDeleted(true);
            currentSession().update(column);
        }
        table.setDeleted(true);
        currentSession().update(table);
    }

    public void update(com.flipkart.fdp.superbi.cosmos.meta.model.external.Table table) {
        Table internalTable = null;
        try{
            internalTable = getTableByName(table.getName());
        }catch (Exception e){
        }
        if(internalTable==null){
            save(table);
        }
        else {
            int newVersion = internalTable.getVersion()+1;
            delete(table.getName());
            save(table,newVersion);
        }
    }

    private List<Table> getAllVersions(String name){
        return list(criteria().add(eq("name", name)));
    }

    private Table getExistingTable(String name){
        Table table = null;
        List<Table> tables = getAllVersions(name);
        if (!tables.isEmpty()){
            Ordering<Table> byVersion = new Ordering<Table>() {
                @Override
                public int compare(@Nullable Table left, @Nullable Table right) {
                    return Ints.compare(left.getVersion(),right.getVersion());
                }
            };
            Collections.sort(tables,byVersion);
            table = tables.get(tables.size()-1);
        }
        return table;
    }


    public void updateProcessId(String tableName, int processId) {
        Table internalTable = getTableByName(tableName);
        internalTable.setProcessId(processId);
        currentSession().save(internalTable);
    }

    public void updateSource(String tableName, String sourceName) {
        Table internalTable = getTableByName(tableName);
        internalTable.setSource(getSource(sourceName));
        currentSession().save(internalTable);
    }

    public Date getLastUpdateTime (String tableName) {
        try {
            Table internalTable = uniqueResult(criteria().add(eq("name", tableName)).add(eq("deleted", false)).addOrder(Order.desc("lastUpdate")).setMaxResults(1));
            return internalTable.getLastUpdate();
        }
        catch (HibernateException ex) {
            System.out.println("LastUpdateTime failed for table "+tableName);
            return null;
        }
    }

}
