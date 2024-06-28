package com.flipkart.fdp.superbi.cosmos.meta.db.dao;

import com.flipkart.fdp.superbi.cosmos.meta.model.data.DartEntity;
import org.hibernate.SessionFactory;

/**
 * User: aartika
 * Date: 4/16/14
 */
public class DartEntityDAO extends BaseDAO<DartEntity> {

    public DartEntityDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public DartEntity getByUri(String company, String org, String namespace, String name) {

        return uniqueResult(currentSession().createQuery("select de from DartEntity de " +
                "join de.schemas as ss " +
                "join ss.company as com " +
                "join ss.namespace as ns " +
                "join ns.org as o " +
                "where com.name = :company and " +
                "o.name=:org and " +
                "ns.name=:namespace and " +
                "ss.name=:name")
                .setString("company", company)
                .setString("org", org)
                .setString("namespace", namespace)
                .setString("name", name));
    }

    public DartEntity getByUriAndVersion(String company, String org, String namespace, String name,String dartVersion) {

        return uniqueResult(currentSession().createQuery("select de from DartEntity de " +
                "join de.schemas as ss " +
                "join ss.company as com " +
                "join ss.namespace as ns " +
                "join ns.org as o " +
                "where com.name = :company and " +
                "o.name=:org and " +
                "ns.name=:namespace and " +
                "ss.name=:name and " +
                "ss.dartVersion=:dartVersion")
                .setString("company", company)
                .setString("org", org)
                .setString("namespace", namespace)
                .setString("name", name)
                .setString("dartVersion",dartVersion));
    }
}
