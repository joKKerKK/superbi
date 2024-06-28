package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.BoltLookup;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Table;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebBoltLookup;
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
 * Date: 8/21/14
 * Time: 5:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class BoltLookupDAO extends AbstractDAO<BoltLookup> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public BoltLookupDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
    public void save(WebBoltLookup boltLookup){
        BoltLookup internalBoltLookup = getExistingBoltLookup(boltLookup);
        if(internalBoltLookup == null){
            save(boltLookup,1);
        }
        else{
            internalBoltLookup.setDeleted(true);
            currentSession().save(internalBoltLookup);
            save(boltLookup,internalBoltLookup.getVersion()+1);
        }
    }

    private void save (WebBoltLookup webBoltLookup, int version){
        BoltLookup boltLookup = new BoltLookup();
        boltLookup.setLookupName(webBoltLookup.getLookupName());
        boltLookup.setLookupType(webBoltLookup.getLookupType());
        boltLookup.setDeleted(false);
//        Table factTable = getTableFromName(webBoltLookup.getFactTableName());
        boltLookup.settName(webBoltLookup.getFactTableName());
        boltLookup.setLookupTableName(webBoltLookup.getLookupTableName());
        boltLookup.setIdExpression(webBoltLookup.getIdExpression());
        boltLookup.setFilterExpression(webBoltLookup.getFilterExpression());
        boltLookup.setIdDataType(webBoltLookup.getIdDataType());
        boltLookup.setTimestampExpression(webBoltLookup.getTimestampExpression());
        boltLookup.setTimestampDataType(webBoltLookup.getTimestampDataType());
        boltLookup.setVersionExpression(webBoltLookup.getVersionExpression());
        boltLookup.setVersionDataType(webBoltLookup.getVersionDataType());
        boltLookup.setVersion(version);

        currentSession().save(boltLookup);
    }

    public void delete(String tableName){
        List<BoltLookup> boltLookups = getAllBoltLookupByTableName(tableName);
        for(BoltLookup boltLookup : boltLookups){
            boltLookup.setDeleted(true);
            currentSession().update(boltLookup);
        }
    }

    public void delete(String tableName, String lookupTableName, String lookupName){
        BoltLookup boltLookup = getBoltLookupByNames(tableName,lookupTableName,lookupName);
        if(boltLookup !=null){
            boltLookup.setDeleted(true);
            currentSession().update(boltLookup);
        }
    }

    public Set<BoltLookup> getAllBoltLookup(){
        return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
    }

    public List<BoltLookup> getAllBoltLookupByLookupTableName(String lookupTableName){
        List<BoltLookup> boltLookups = list(criteria().add(eq("lookupTableName", lookupTableName)).add(eq("deleted", false)));
        if(boltLookups.size() == 0)
            throw new RuntimeException("no bolt lookup with criteria : lookup table name= "+ lookupTableName);
        return boltLookups;
    }

    public List<BoltLookup> getAllBoltLookupByTableName(String factTableName){
        Table table = getTableFromName(factTableName);
        List<BoltLookup> boltLookups = list(criteria().add(eq("tName", factTableName)).add(eq("deleted", false)));
        if(boltLookups.size() == 0)
            throw new RuntimeException("no bolt lookup with criteria : table name= "+ factTableName);
        return boltLookups;
    }
    public BoltLookup getBoltLookupByNames(String factTableName, String lookupTableName, String lookupName){
        BoltLookup boltLookup = null;
        List<BoltLookup> boltLookups = list(criteria().add(eq("tName", factTableName)).add(eq("lookupTableName", lookupTableName)).add(eq("lookupName", lookupName)).add(eq("deleted", false)));
        if (!boltLookups.isEmpty()){
            Ordering<BoltLookup> byVersion = new Ordering<BoltLookup>() {
                @Override
                public int compare(@Nullable BoltLookup left, @Nullable BoltLookup right) {
                    return Ints.compare(left.getVersion(), right.getVersion());
                }
            };
            Collections.sort(boltLookups, byVersion);
            boltLookup = boltLookups.get(boltLookups.size()-1);
        }
        return boltLookup;
    }

    public void update(WebBoltLookup webBoltLookup){
        BoltLookup  boltLookup = null;
        try{
            boltLookup =getBoltLookupByNames(webBoltLookup.getFactTableName(),webBoltLookup.getLookupTableName(),webBoltLookup.getLookupName());
        }catch (Exception e){
        }
        if(boltLookup==null){
            save(webBoltLookup);
        }
        else {
            int newVersion = boltLookup.getVersion()+1;
            boltLookup.setDeleted(true);
            currentSession().save(boltLookup);
            save(webBoltLookup,newVersion);
        }
    }

    private List<BoltLookup> getAllVersions(WebBoltLookup webBoltLookup){
        return list(criteria().add(eq("lookupTableName",webBoltLookup.getLookupTableName())).add(eq("lookupName",webBoltLookup.getLookupName())).add(eq("tName",webBoltLookup.getFactTableName())));
    }

    private BoltLookup getExistingBoltLookup(WebBoltLookup webBoltLookup){
        BoltLookup boltLookup = null;
        List<BoltLookup> boltLookups = getAllVersions(webBoltLookup);
        if (!boltLookups.isEmpty()){
            Ordering<BoltLookup> byVersion = new Ordering<BoltLookup>() {
                @Override
                public int compare(@Nullable BoltLookup left, @Nullable BoltLookup right) {
                    return Ints.compare(left.getVersion(), right.getVersion());
                }
            };
            Collections.sort(boltLookups, byVersion);
            boltLookup = boltLookups.get(boltLookups.size()-1);
        }
        return boltLookup;
    }

    private Table getTableFromName(String tableName){
        TableDAO tableDAO = new TableDAO(this.getSessionFactory());
        Table table = tableDAO.getTableByName(tableName);
        return table;
    }



}
