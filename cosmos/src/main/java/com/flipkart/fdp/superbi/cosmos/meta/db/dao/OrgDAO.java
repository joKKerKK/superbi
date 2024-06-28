package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Namespace;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Org;
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
 * Date: 03/04/14
 * Time: 4:57 PM
 */
public class OrgDAO extends AbstractDAO<Org> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public OrgDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(String orgName){
        Org org = getExistingOrg(orgName);
        if(org == null){
            save(orgName,1);
        }
        else{
            save(orgName,org.getVersion()+1);
        }
    }

    private void save (String orgName, int version){
        Org org = new Org(orgName,version);
        currentSession().save(org);
    }

    public void delete(String name){
        Org org = getOrgByName(name);
        for(Namespace namespace : org.getNamespaces()){
            namespace.setDeleted(true);
            currentSession().update(namespace);
        }
        org.setDeleted(true);
        currentSession().update(org);

    }

    public Set<Org> getOrgs(){
        return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
    }

    public Org getOrgByName(String orgName){
        Org org = uniqueResult(criteria().add(eq("name",orgName)).add(eq("deleted",false)));
        if(org == null)
            throw new RuntimeException("no org by name : "+ orgName);
        return org;
    }

    private List<Org> getAllVersions(String name){
        return list(criteria().add(eq("name", name)));
    }

    private Org getExistingOrg(String name){
        Org org = null;
        List<Org> orgs = getAllVersions(name);
        if (!orgs.isEmpty()){
            Ordering<Org> byVersion = new Ordering<Org>() {
                @Override
                public int compare(@Nullable Org left, @Nullable Org right) {
                    return Ints.compare(left.getVersion(), right.getVersion());
                }
            };
            Collections.sort(orgs,byVersion);
            org = orgs.get(orgs.size()-1);
        }
        return org;
    }
}
