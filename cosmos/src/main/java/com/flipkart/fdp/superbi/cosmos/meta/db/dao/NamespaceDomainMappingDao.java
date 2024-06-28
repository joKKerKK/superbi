package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.api.MetaAccessor;
import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Namespace;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.NamespaceDomainMapping;
import com.flipkart.fdp.superbi.cosmos.meta.model.external.JasperDomain;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.hibernate.SessionFactory;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 21/04/14
 * Time: 9:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class NamespaceDomainMappingDao extends AbstractDAO<NamespaceDomainMapping> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public NamespaceDomainMappingDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
    
    private void save(NamespaceDomainMapping mapping){
        NamespaceDomainMapping existingMapping =  getExisting(mapping.getNamespace());
        if(existingMapping== null){
            mapping.setVersion(1);
            currentSession().save(mapping);
        }
        else {
            /* THE TABLE WOULD HAVE ONLY THE LATEST DOMAIN
            (every time a fact or dim gets created in a namespace, a new version
            will get added which might be unnecessary)
             */
            existingMapping.setDomainSchemaXML(mapping.getDomainSchemaXML());
            existingMapping.setDomainSecurityXML(mapping.getDomainSecurityXML());
            existingMapping.setVersion(existingMapping.getVersion() + 1);
            currentSession().update(existingMapping);
        }
    }

    private List<NamespaceDomainMapping> getAllVersions(Namespace namespace){
        return list(criteria().add(eq("namespace", namespace)));
    }

    private NamespaceDomainMapping getExisting(Namespace namespace){
        NamespaceDomainMapping mapping = null;
        List<NamespaceDomainMapping> mappings = getAllVersions(namespace);
        if (!mappings.isEmpty()){
            Ordering<NamespaceDomainMapping> byVersion = new Ordering<NamespaceDomainMapping>() {
                @Override
                public int compare(@Nullable NamespaceDomainMapping left, @Nullable NamespaceDomainMapping right) {
                    return Ints.compare(left.getVersion(), right.getVersion());
                }
            };
            Collections.sort(mappings, byVersion);
            mapping = mappings.get(mappings.size()-1);
        }
        return mapping;
    }

    public void refreshFor (String org, String namespace) {
        Namespace internal = new NamespaceDAO(getSessionFactory()).getNamespaceByOrgAndName(
                org,
                namespace);
        JasperDomain domain = JasperDomain.createFor(new MetaAccessor(getSessionFactory()), namespace);
        NamespaceDomainMapping mapping = new NamespaceDomainMapping(
                internal,
                domain.schemaXML,
                domain.securityXML
        );
        save(mapping);
    }

    public NamespaceDomainMapping getFor(String org, String namespace) {
        final Namespace internal = new NamespaceDAO(getSessionFactory()).getNamespaceByOrgAndName(
                        org,
                        namespace);
        return getExisting(internal);
    }

    public void delete(String name, String orgName){
        final NamespaceDomainMapping mapping = getFor(orgName,name);
        mapping.setDeleted(true);
        currentSession().update(mapping);
    }
}
