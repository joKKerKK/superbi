package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Namespace;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Org;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.View;
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
 * Time: 5:25 PM
 */
public class NamespaceDAO extends AbstractDAO<Namespace> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public NamespaceDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(View.Namespace namespace){
        Namespace internalNamespace =  getExistingNamespace(namespace.getNamespace());
        if(internalNamespace == null){
            save(namespace,1);
        }
        else
            save(namespace,internalNamespace.getVersion()+1);
    }

    private void save(View.Namespace namespace, int version){
        OrgDAO orgDao = new OrgDAO(getSessionFactory());
        Org org = orgDao.getOrgByName(namespace.getOrg());
        Namespace internalNamespace = new Namespace(namespace.getNamespace(),org,version);
        currentSession().save(internalNamespace);
    }

    private List<Namespace> getAllVersions(String name){
        return list(criteria().add(eq("name", name)));
    }

    private Namespace getExistingNamespace(String name){
        Namespace namespace = null;
        List<Namespace> namespaces = getAllVersions(name);
        if (!namespaces.isEmpty()){
            Ordering<Namespace> byVersion = new Ordering<Namespace>() {
                @Override
                public int compare(@Nullable Namespace left, @Nullable Namespace right) {
                    return Ints.compare(left.getVersion(),right.getVersion());
                }
            };
            Collections.sort(namespaces,byVersion);
            namespace = namespaces.get(namespaces.size()-1);
        }
        return namespace;
    }

    public Set<Namespace> getNamespaces(){
        return Sets.newHashSet(list(criteria().add(eq("deleted", false))));
    }

    private Namespace getNamespaceByName(String name){
        Namespace namespace = uniqueResult(criteria().add(eq("name",name)).add(eq("deleted",false)));
        if(namespace == null)
            throw new RuntimeException("no source type by name : "+ name);
        return namespace;
    }

//    public Namespace getNamespaceByOrgAndName(String org, String name) {
//        return uniqueResult(criteria().createAlias("org", "org").add(eq("org.name", org)).add(eq("name", name)));
//    }

    public Namespace getNamespaceByOrgAndName(String orgName, String name){
        OrgDAO orgDao = new OrgDAO(getSessionFactory());
        Org org = orgDao.getOrgByName(orgName);
        Namespace namespace = uniqueResult(criteria().add(eq("name",name)).add(eq("deleted",false)).add(eq("org",org)));
        if(namespace == null)
            throw new RuntimeException("no namespace by name : "+ name);
        return namespace;
    }

    public void delete(String name, String orgName){
        Namespace namespace = getNamespaceByOrgAndName(orgName,name);
        namespace.setDeleted(true);
        currentSession().update(namespace);
        postDelete(orgName, name);
    }

    private void postDelete(String org, String namespace) {
        new NamespaceDomainMappingDao(
                getSessionFactory()).delete(org, namespace);
    }

}
