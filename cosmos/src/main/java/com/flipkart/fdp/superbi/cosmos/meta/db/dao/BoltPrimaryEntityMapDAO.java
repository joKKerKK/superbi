package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.BoltPrimaryEntityMap;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebBoltPrimaryEntityMap;
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
 * Date: 9/1/14
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class BoltPrimaryEntityMapDAO extends AbstractDAO<BoltPrimaryEntityMap> {


    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public BoltPrimaryEntityMapDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(WebBoltPrimaryEntityMap boltPrimaryEntityMap){
            BoltPrimaryEntityMap internalBoltPrimaryEntityMap = getExistingBoltPrimaryEntityMap(boltPrimaryEntityMap);
            if(internalBoltPrimaryEntityMap == null){
                save(boltPrimaryEntityMap,1);
            }
            else{
                internalBoltPrimaryEntityMap.setDeleted(true);
                currentSession().save(internalBoltPrimaryEntityMap);
                save(boltPrimaryEntityMap,internalBoltPrimaryEntityMap.getVersion()+1);
            }
        }

        private void save (WebBoltPrimaryEntityMap webBoltPrimaryEntityMap, int version){
            BoltPrimaryEntityMap boltPrimaryEntityMap = new BoltPrimaryEntityMap();
            boltPrimaryEntityMap.setId(webBoltPrimaryEntityMap.getId());
            boltPrimaryEntityMap.setTableName(webBoltPrimaryEntityMap.getTableName());
            boltPrimaryEntityMap.setEntityCompany(webBoltPrimaryEntityMap.getEntityCompany());
            boltPrimaryEntityMap.setEntityOrg(webBoltPrimaryEntityMap.getEntityOrg());
            boltPrimaryEntityMap.setEntityNamespace(webBoltPrimaryEntityMap.getEntityNamespace());
            boltPrimaryEntityMap.setEntityName(webBoltPrimaryEntityMap.getEntityName());
            boltPrimaryEntityMap.setConfig(webBoltPrimaryEntityMap.getConfig());
            boltPrimaryEntityMap.setDeleted(false);
            boltPrimaryEntityMap.setVersion(version);
            currentSession().save(boltPrimaryEntityMap);
        }

        public void delete(String tableName){
            List<BoltPrimaryEntityMap> boltPrimaryEntityMaps = getBoltPrimaryEntityMapByTable(tableName);
            for(BoltPrimaryEntityMap boltPrimaryEntityMap : boltPrimaryEntityMaps){
                boltPrimaryEntityMap.setDeleted(true);
                currentSession().update(boltPrimaryEntityMap);
            }

        }


        public void update(WebBoltPrimaryEntityMap webBoltPrimaryEntityMap){
            BoltPrimaryEntityMap  boltPrimaryEntityMap = null;
            try{
                boltPrimaryEntityMap = getBoltPrimaryEntityMap(webBoltPrimaryEntityMap.getTableName());
            }catch (Exception e){
            }
            if(boltPrimaryEntityMap==null){
                save(webBoltPrimaryEntityMap);
            }
            else {
                int newVersion = boltPrimaryEntityMap.getVersion()+1;
                boltPrimaryEntityMap.setDeleted(true);
                currentSession().save(boltPrimaryEntityMap);
                save(webBoltPrimaryEntityMap,newVersion);
            }
        }

        public Set<BoltPrimaryEntityMap> getAllBoltPrimaryEntityMap(){
            return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
        }

        public List<BoltPrimaryEntityMap> getBoltPrimaryEntityMapByTable(String tableName){
            List<BoltPrimaryEntityMap> boltPrimaryEntityMaps = list(criteria().add(eq("tableName",tableName)).add(eq("deleted",false)));
            if(boltPrimaryEntityMaps.size() == 0)
                throw new RuntimeException("no bolt lookup with criteria : table name= "+ tableName);
            return boltPrimaryEntityMaps;
        }

        public BoltPrimaryEntityMap getBoltPrimaryEntityMap(String tableName){
            BoltPrimaryEntityMap boltPrimaryEntityMap = null;
            List<BoltPrimaryEntityMap> boltPrimaryEntityMaps = list(criteria().add(eq("tableName", tableName)).add(eq("deleted",false)));
            if (!boltPrimaryEntityMaps.isEmpty()){
                Ordering<BoltPrimaryEntityMap> byVersion = new Ordering<BoltPrimaryEntityMap>() {
                    @Override
                    public int compare(@Nullable BoltPrimaryEntityMap left, @Nullable BoltPrimaryEntityMap right) {
                        return Ints.compare(left.getVersion(), right.getVersion());
                    }
                };
                Collections.sort(boltPrimaryEntityMaps, byVersion);
                boltPrimaryEntityMap = boltPrimaryEntityMaps.get(boltPrimaryEntityMaps.size()-1);
            }
            return boltPrimaryEntityMap;
        }

        private List<BoltPrimaryEntityMap> getAllVersions(WebBoltPrimaryEntityMap webBoltPrimaryEntityMap){
            return list(criteria().add(eq("tableName",webBoltPrimaryEntityMap.getTableName())));
        }

        private BoltPrimaryEntityMap getExistingBoltPrimaryEntityMap(WebBoltPrimaryEntityMap webBoltPrimaryEntityMap){
            BoltPrimaryEntityMap boltPrimaryEntityMap = null;
            List<BoltPrimaryEntityMap> boltPrimaryEntityMaps = getAllVersions(webBoltPrimaryEntityMap);
            if (!boltPrimaryEntityMaps.isEmpty()){
                Ordering<BoltPrimaryEntityMap> byVersion = new Ordering<BoltPrimaryEntityMap>() {
                    @Override
                    public int compare(@Nullable BoltPrimaryEntityMap left, @Nullable BoltPrimaryEntityMap right) {
                        return Ints.compare(left.getVersion(), right.getVersion());
                    }
                };
                Collections.sort(boltPrimaryEntityMaps, byVersion);
                boltPrimaryEntityMap = boltPrimaryEntityMaps.get(boltPrimaryEntityMaps.size()-1);
            }
            return boltPrimaryEntityMap;
        }

}

