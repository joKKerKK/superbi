package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.BoltSchemaMap;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Table;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebBoltSchemaMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.hibernate.SessionFactory;

/**
 * Created with IntelliJ IDEA.
 * User: debjyoti.paul
 * Date: 8/22/14
 * Time: 11:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class BoltSchemaMapDAO extends AbstractDAO<BoltSchemaMap> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public BoltSchemaMapDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(WebBoltSchemaMap boltSchemaMap){
        BoltSchemaMap internalBoltSchemaMap = getExistingBoltSchemaMap(boltSchemaMap);
        if(internalBoltSchemaMap == null){
            save(boltSchemaMap,1);
        }
        else{
            internalBoltSchemaMap.setDeleted(true);
            currentSession().save(internalBoltSchemaMap);
            save(boltSchemaMap,internalBoltSchemaMap.getVersion()+1);
        }
    }

    private void save (WebBoltSchemaMap webBoltSchemaMap, int version){
        BoltSchemaMap boltSchemaMap = new BoltSchemaMap();
//        Table table = getTableFromName(webBoltSchemaMap.getFactTableName());
        boltSchemaMap.setColumnName(webBoltSchemaMap.getColumnName());
        boltSchemaMap.settName(webBoltSchemaMap.getFactTableName());
        boltSchemaMap.setColumnDefinition(webBoltSchemaMap.getColumnDefinition());
        boltSchemaMap.setColumnType(webBoltSchemaMap.getColumnType());
        boltSchemaMap.setDeleted(false);
        boltSchemaMap.setVersion(version);

        currentSession().save(boltSchemaMap);
    }

    public void delete(String tableName){
        List<BoltSchemaMap> boltSchemaMaps = getBoltSchemaMapsByTable(tableName);
        for(BoltSchemaMap boltLookup : boltSchemaMaps){
            boltLookup.setDeleted(true);
            currentSession().update(boltLookup);
        }

    }

    public void delete(String tableName, String columnName){
        BoltSchemaMap boltSchemaMap = getBoltSchemaMap(tableName,columnName);
        if(boltSchemaMap !=null){
            boltSchemaMap.setDeleted(true);
            currentSession().update(boltSchemaMap);
        }
    }

    public void update(WebBoltSchemaMap webBoltSchemaMap){
        BoltSchemaMap  boltSchemaMap = null;
        try{
            boltSchemaMap = getBoltSchemaMap(webBoltSchemaMap.getFactTableName(), webBoltSchemaMap.getColumnName());
        }catch (Exception e){
        }
        if(boltSchemaMap==null){
            save(webBoltSchemaMap);
        }
        else {
            int newVersion = boltSchemaMap.getVersion()+1;
            boltSchemaMap.setDeleted(true);
            currentSession().save(boltSchemaMap);
            save(webBoltSchemaMap,newVersion);
        }
    }

    public Set<BoltSchemaMap> getAllBoltLookup(){
        return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
    }

    public List<BoltSchemaMap> getBoltSchemaMapsByTable(String tableName){
        List<BoltSchemaMap> boltSchemaMaps = list(criteria().add(eq("tName",tableName)).add(eq("deleted",false)));
        if(boltSchemaMaps.size() == 0)
            throw new RuntimeException("no bolt lookup with criteria : table name= "+ tableName);
        return boltSchemaMaps;
    }

    public BoltSchemaMap getBoltSchemaMap(String tableName, String columnName){
        BoltSchemaMap boltSchemaMap = null;
        List<BoltSchemaMap> boltSchemaMaps = list(criteria().add(eq("tName", tableName)).add(eq("columnName",columnName)).add(eq("deleted",false)));
        if (!boltSchemaMaps.isEmpty()){
            Ordering<BoltSchemaMap> byVersion = new Ordering<BoltSchemaMap>() {
                @Override
                public int compare(@Nullable BoltSchemaMap left, @Nullable BoltSchemaMap right) {
                    return Ints.compare(left.getVersion(), right.getVersion());
                }
            };
            Collections.sort(boltSchemaMaps, byVersion);
            boltSchemaMap = boltSchemaMaps.get(boltSchemaMaps.size()-1);
        }
        return boltSchemaMap;
    }

    private List<BoltSchemaMap> getAllVersions(WebBoltSchemaMap webBoltSchemaMap){
        return list(criteria().add(eq("tName",webBoltSchemaMap.getFactTableName())));
    }

    private BoltSchemaMap getExistingBoltSchemaMap(WebBoltSchemaMap webBoltSchemaMap){
        BoltSchemaMap boltSchemaMap = null;
        List<BoltSchemaMap> boltSchemaMaps = getAllVersions(webBoltSchemaMap);
        if (!boltSchemaMaps.isEmpty()){
            Ordering<BoltSchemaMap> byVersion = new Ordering<BoltSchemaMap>() {
                @Override
                public int compare(@Nullable BoltSchemaMap left, @Nullable BoltSchemaMap right) {
                    return Ints.compare(left.getVersion(), right.getVersion());
                }
            };
            Collections.sort(boltSchemaMaps, byVersion);
            boltSchemaMap = boltSchemaMaps.get(boltSchemaMaps.size()-1);
        }
        return boltSchemaMap;
    }

    private Table getTableFromName(String tableName){
        TableDAO tableDAO = new TableDAO(this.getSessionFactory());
        Table table = tableDAO.getTableByName(tableName);
        return table;
    }
}
