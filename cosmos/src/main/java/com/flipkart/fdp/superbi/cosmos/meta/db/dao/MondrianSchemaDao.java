package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import static org.hibernate.criterion.Restrictions.and;
import static org.hibernate.criterion.Restrictions.eq;

import com.flipkart.fdp.superbi.cosmos.meta.db.AbstractDAO;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.MondrianSchema;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Namespace;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Org;
import com.flipkart.fdp.superbi.cosmos.meta.model.data.Source;
import com.google.common.base.Optional;
import java.util.List;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;

/**
 * Created with IntelliJ IDEA.
 * User: amruth.s
 * Date: 21/04/14
 */

public class MondrianSchemaDao extends AbstractDAO<MondrianSchema> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public MondrianSchemaDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void save(MondrianSchema mapping){
        Optional<MondrianSchema> existingOptional =  getLastButNthVersion(mapping.getNamespace(), mapping.getSource(), 0);
        if(existingOptional.isPresent()){
            final MondrianSchema existing = existingOptional.get();
            existing.setDeleted(true);
            currentSession().update(existing);

            mapping.setVersion(existing.getVersion() + 1);
            currentSession().save(mapping);
        }
        else {
            mapping.setVersion(1);
            currentSession().save(mapping);
        }
    }

    public Optional<MondrianSchema> getLastButNthVersion(String org, String namespace, String sourceName, int n){
        final Namespace namespaceObj = new NamespaceDAO(getSessionFactory()).getNamespaceByOrgAndName(
            org,
            namespace);
        final Source source = new SourceDAO(getSessionFactory()).getSourceByName(
            sourceName);
        return getLastButNthVersion(namespaceObj, source, n);
    }

    /* Shows the latest non deleted */
    public Optional<MondrianSchema> getLatest(String org, String namespace, String sourceName){
        final Namespace namespaceObj = new NamespaceDAO(getSessionFactory()).getNamespaceByOrgAndName(
            org,
            namespace);
        final Source source = new SourceDAO(getSessionFactory()).getSourceByName(
            sourceName);
        final Criteria criteria = criteria().add(and(eq("namespace", namespaceObj), eq("source", source), eq("deleted", false)));
        criteria.addOrder(Order.desc("version"));
        criteria.setMaxResults(1);
        List<MondrianSchema> resultList = list(criteria);
        return resultList.size() == 0 ?
            Optional.<MondrianSchema>absent():
            Optional.of(list(criteria).get(0));
    }

    /* This api can browse through delete versions */
    private Optional<MondrianSchema> getLastButNthVersion(Namespace namespace, Source source, int n) {
        Criteria criteria = criteria().add(and(eq("namespace", namespace), eq("source", source)));
        criteria.addOrder(Order.desc("version"));
        criteria.setFirstResult(n);
        criteria.setMaxResults(1);
        List<MondrianSchema> resultList = list(criteria);
        return resultList.size() == 0 ?
            Optional.<MondrianSchema>absent():
            Optional.of(list(criteria).get(0));
    }

    public List<MondrianSchema> getBy(Namespace namespace) {
        return list(
            criteria().add(and(eq("namespace", namespace), eq("deleted", false)))
        );
    }

    public List<MondrianSchema> getBy(Org org) {
        return list(
            criteria()
                .createAlias("namespace", "n")
                .createAlias("n.org", "o")
                .add(
                    and(
                        eq("o.id", org.getId()),
                        eq("deleted", false)
                    )
                )
        );
    }


    public void delete(String orgName, String namespace, String sourceName){
        final Optional<MondrianSchema> mappingOptional = getLatest(orgName, namespace, sourceName);
        if(mappingOptional.isPresent()) {
            final MondrianSchema mapping = mappingOptional.get();
            mapping.setDeleted(true);
            currentSession().update(mapping);
        } else {
            throw new RuntimeException("We don't have such a thing !");
        }
    }

    public List<MondrianSchema> getAll() {
        return list(criteria().add(eq("deleted", false)));
    }
}
